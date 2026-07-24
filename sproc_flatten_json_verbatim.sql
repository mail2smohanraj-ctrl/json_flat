-- sproc_flatten_json_verbatim.sql
--
-- Wraps the ORIGINAL flatten_json.py functions UNCHANGED inside a Snowpark
-- Python stored procedure. Every function below - sanitize_name,
-- register_row, flatten_object, infer_snowflake_type, write_outputs - is
-- copied verbatim from the uploaded script, byte for byte. Nothing about
-- the flattening/type-inference logic was touched.
--
-- Only new code (clearly marked below) was added around it, because a
-- stored procedure needs different plumbing than a CLI script:
--   - INPUT:  the original script did `json.load(open(path))` from a real
--             file path. A stored procedure has no access to your local
--             disk, so it takes the JSON as a STRING parameter instead
--             (read from a landing table, or passed directly).
--   - OUTPUT: the original script's write_outputs() writes real .csv and
--             .sql files to a directory - THIS STILL HAPPENS, unmodified,
--             to a temp directory that Snowflake's sandboxed Python
--             runtime provides. The new code then reads those exact same
--             files back and pushes them into Snowflake:
--               * uploads each generated CSV to an internal stage via
--                 session.file.put_stream()  (the in-process equivalent
--                 of running `PUT` from snowsql)
--               * runs each generated CREATE TABLE statement, but checks
--                 INFORMATION_SCHEMA first and skips/ALTERs instead if the
--                 table already exists
--               * runs COPY INTO using the generated .columns file for the
--                 exact column list
--
-- Deploy:
--   snowsql -f sproc_flatten_json_verbatim.sql
--
-- Call:
--   CALL FLATTEN_JSON_VERBATIM(
--     (SELECT raw_data::STRING FROM raw_sample LIMIT 1),  -- JSON text
--     'sample',                 -- root table name
--     'sample.json',            -- source file label (lineage)
--     'JSON_LOAD_STAGE'         -- stage to use for the generated CSVs
--   );

CREATE OR REPLACE PROCEDURE FLATTEN_JSON_VERBATIM(
    JSON_TEXT STRING,
    ROOT_TABLE_NAME STRING,
    SOURCE_FILE_LABEL STRING,
    STAGE_NAME STRING
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
# =============================================================================
# EVERYTHING BETWEEN THIS LINE AND THE "END OF VERBATIM SECTION" MARKER BELOW
# IS COPIED EXACTLY FROM THE UPLOADED flatten_json.py - NOT REWRITTEN.
# (module-level docstring omitted here only to keep the CREATE PROCEDURE
# header comment above from duplicating it; the code itself is untouched)
# =============================================================================
import csv
import json
import os
import re
import sys
import uuid
from collections import OrderedDict, defaultdict

# ---------------------------------------------------------------------------
# Global state (per run, across all input files so schemas merge)
# ---------------------------------------------------------------------------
tables = defaultdict(list)                 # table_name -> list[row dict]
table_columns_order = defaultdict(list)    # table_name -> [col1, col2, ...] first-seen order
table_column_types = defaultdict(lambda: defaultdict(set))   # table -> col -> set(python types seen)
table_column_maxlen = defaultdict(lambda: defaultdict(int))  # table -> col -> max string length seen
table_parent = {}                          # table_name -> parent_table_name (or None)
table_order = []                           # first-seen order = safe creation order


def sanitize_name(name):
    """Make a string safe to use as a Snowflake table/column identifier."""
    name = re.sub(r"[^0-9a-zA-Z_]", "_", str(name)).strip("_")
    if not name:
        name = "col"
    if name[0].isdigit():
        name = f"c_{name}"
    return name.lower()


def register_row(table_name, row, parent_table):
    """Store a row, track column order, and update per-column type/length stats."""
    if table_name not in table_order:
        table_order.append(table_name)
        table_parent[table_name] = parent_table

    for col, val in row.items():
        if col not in table_columns_order[table_name]:
            table_columns_order[table_name].append(col)
        if val is not None:
            table_column_types[table_name][col].add(type(val))
            if isinstance(val, str):
                table_column_maxlen[table_name][col] = max(
                    table_column_maxlen[table_name][col], len(val)
                )

    tables[table_name].append(row)


def flatten_object(obj, table_name, parent_col=None, parent_id=None, source_file=None, parent_table=None):
    """Flatten one JSON object into `table_name`, recursing into nested
    lists/objects to populate child tables. Returns the row's surrogate id.
    `parent_table` is the name of the table that "owns" this table (None for
    a top-level/root table) - it is recorded once, on first registration.
    """
    row = OrderedDict()
    row_id_col = f"{table_name}_id"
    row_id = str(uuid.uuid4())
    row[row_id_col] = row_id
    if parent_col is not None:
        row[parent_col] = parent_id
    row["_source_file"] = source_file

    child_work = []  # (child_field_name, list_value) to process after scalars are set

    def walk(d, prefix):
        for raw_key, val in d.items():
            key = sanitize_name(raw_key)
            colname = key if not prefix else f"{prefix}_{key}"
            if isinstance(val, dict):
                walk(val, colname)
            elif isinstance(val, list):
                child_work.append((colname, val))
            else:
                row[colname] = val

    if isinstance(obj, dict):
        walk(obj, "")
    else:
        # A bare scalar/list inside a list-of-mixed-types: store as "value"
        row["value"] = obj

    register_row(table_name, row, parent_table=parent_table)

    for colname, val_list in child_work:
        child_table = f"{table_name}__{colname}"
        if len(val_list) == 0:
            continue
        if all(not isinstance(x, (dict, list)) for x in val_list):
            # list of scalars -> child table with a single "value" column
            for v in val_list:
                crow = OrderedDict()
                crow[f"{child_table}_id"] = str(uuid.uuid4())
                crow[row_id_col] = row_id
                crow["value"] = v
                crow["_source_file"] = source_file
                register_row(child_table, crow, parent_table=table_name)
        else:
            for item in val_list:
                if isinstance(item, dict):
                    flatten_object(
                        item, child_table,
                        parent_col=row_id_col, parent_id=row_id,
                        source_file=source_file, parent_table=table_name,
                    )
                elif isinstance(item, list):
                    # nested list-of-lists: rare, store as JSON text
                    crow = OrderedDict()
                    crow[f"{child_table}_id"] = str(uuid.uuid4())
                    crow[row_id_col] = row_id
                    crow["value_json"] = json.dumps(item)
                    crow["_source_file"] = source_file
                    register_row(child_table, crow, parent_table=table_name)
                else:
                    crow = OrderedDict()
                    crow[f"{child_table}_id"] = str(uuid.uuid4())
                    crow[row_id_col] = row_id
                    crow["value"] = item
                    crow["_source_file"] = source_file
                    register_row(child_table, crow, parent_table=table_name)

    return row_id


# ---------------------------------------------------------------------------
# Type inference: python types observed -> Snowflake column type
# ---------------------------------------------------------------------------
def infer_snowflake_type(table, col):
    types_seen = table_column_types[table][col]
    types_seen = types_seen - {type(None)}

    if not types_seen:
        return "VARCHAR(255)"  # column always NULL/empty so far; safe default

    if types_seen <= {bool}:
        return "BOOLEAN"
    if types_seen <= {int}:
        return "NUMBER(38,0)"
    if types_seen <= {int, float} or types_seen <= {float}:
        return "FLOAT"
    if types_seen <= {int, float, bool}:
        return "FLOAT"

    # Anything involving str, or a genuinely mixed bag -> VARCHAR (safe fallback)
    maxlen = table_column_maxlen[table][col]
    size = max(64, min(16000, ((maxlen // 64) + 2) * 64))
    return f"VARCHAR({size})"


# ---------------------------------------------------------------------------
# DDL / CSV / manifest generation
# ---------------------------------------------------------------------------
def write_outputs(output_dir):
    staging_dir = os.path.join(output_dir, "staging")
    ddl_dir = os.path.join(output_dir, "ddl")
    os.makedirs(staging_dir, exist_ok=True)
    os.makedirs(ddl_dir, exist_ok=True)

    for table in table_order:
        cols = table_columns_order[table]
        rows = tables[table]

        # --- CSV ---
        csv_path = os.path.join(staging_dir, f"{table}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(cols)
            for row in rows:
                writer.writerow([row.get(c, "") if row.get(c, "") is not None else "" for c in cols])

        # --- column list (exact CSV order, used for COPY INTO) ---
        with open(os.path.join(staging_dir, f"{table}.columns"), "w", encoding="utf-8") as f:
            f.write(",".join(cols))

        # --- DDL ---
        id_col = f"{table}_id"
        parent = table_parent.get(table)
        parent_id_col = f"{parent}_id" if parent else None

        col_defs = []
        for c in cols:
            if c == id_col:
                col_defs.append(f"    {c} VARCHAR(36) PRIMARY KEY")
            elif parent_id_col and c == parent_id_col:
                col_defs.append(f"    {c} VARCHAR(36) REFERENCES {parent}({parent_id_col})")
            elif c == "_source_file":
                col_defs.append(f"    {c} VARCHAR(1024)")
            else:
                col_defs.append(f"    {c} {infer_snowflake_type(table, c)}")

        ddl = f"CREATE TABLE {table} (\n" + ",\n".join(col_defs) + "\n);\n"
        with open(os.path.join(ddl_dir, f"{table}.sql"), "w", encoding="utf-8") as f:
            f.write(ddl)

        print(f"  {table:<40} {len(rows):>6} rows, {len(cols)} cols")

    with open(os.path.join(output_dir, "table_order.txt"), "w", encoding="utf-8") as f:
        f.write("\n".join(table_order) + "\n")
# =============================================================================
# END OF VERBATIM SECTION. Everything below is NEW code: the glue needed to
# run the above inside Snowflake (feed it JSON text instead of a file path,
# then push what write_outputs() already wrote to disk into real tables).
# =============================================================================

import tempfile


def _reset_state():
    """The original script relies on module-level globals (tables,
    table_order, etc.) that persist for the life of the process. Snowflake
    may reuse a warm Python runtime across separate CALLs, so we clear that
    state at the start of every call - otherwise a second CALL in the same
    warm session would see tables/columns left over from a prior call."""
    tables.clear()
    table_columns_order.clear()
    table_column_types.clear()
    table_column_maxlen.clear()
    table_parent.clear()
    table_order.clear()


def _table_exists(session, table_name):
    rows = session.sql(
        f"SELECT COUNT(*) AS CNT FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = CURRENT_SCHEMA() AND TABLE_NAME = UPPER('{table_name}')"
    ).collect()
    return rows[0]["CNT"] > 0


def _existing_columns(session, table_name):
    rows = session.sql(
        f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_SCHEMA = CURRENT_SCHEMA() AND TABLE_NAME = UPPER('{table_name}')"
    ).collect()
    return {r["COLUMN_NAME"].lower() for r in rows}


def _row_count(session, table_name):
    return session.sql(f"SELECT COUNT(*) AS CNT FROM {table_name}").collect()[0]["CNT"]


def _column_def_from_ddl(ddl_text, col):
    """Pull "colname TYPE" back out of the generated CREATE TABLE text,
    for use in an ALTER TABLE ADD COLUMN on an already-existing table."""
    for line in ddl_text.splitlines():
        stripped = line.strip().rstrip(",")
        if stripped.split(" ")[0] == col:
            rest = stripped[len(col):].strip()
            rest = re.sub(r"\s+PRIMARY KEY", "", rest)
            rest = re.sub(r"\s+REFERENCES \w+\(\w+\)", "", rest)
            return rest
    return None


def run(session, json_text, root_table_name, source_file_label, stage_name):
    # 1) Reset the verbatim script's global state for this call.
    _reset_state()

    # 2) Where the original script did json.load(open(path)), we instead
    #    parse the JSON text handed to the procedure.
    doc = json.loads(json_text)

    # 3) Call the ORIGINAL, UNMODIFIED functions exactly as main() did.
    base_table = sanitize_name(root_table_name)
    if isinstance(doc, dict):
        flatten_object(doc, base_table, source_file=source_file_label)
    elif isinstance(doc, list):
        for item in doc:
            flatten_object(item, base_table, source_file=source_file_label)
    else:
        return "Top-level JSON must be an object or array."

    output_dir = tempfile.mkdtemp()
    write_outputs(output_dir)   # <-- the exact, unmodified function; writes real files

    # 4) NEW: push what write_outputs() wrote to disk into real Snowflake
    #    tables (check-before-create, ALTER for drift, then load).
    session.sql(f"CREATE STAGE IF NOT EXISTS {stage_name}").collect()

    summary_lines = []
    for table in table_order:
        ddl_path = os.path.join(output_dir, "ddl", f"{table}.sql")
        csv_path = os.path.join(output_dir, "staging", f"{table}.csv")
        columns_path = os.path.join(output_dir, "staging", f"{table}.columns")

        with open(ddl_path) as f:
            ddl_text = f.read()
        with open(columns_path) as f:
            needed_cols = f.read().split(",")

        if _table_exists(session, table):
            existed = True
            before = _row_count(session, table)
            existing = _existing_columns(session, table)
            added = 0
            for col in needed_cols:
                if col.lower() not in existing:
                    col_def = _column_def_from_ddl(ddl_text, col)
                    if col_def:
                        session.sql(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {col_def}").collect()
                        added += 1
        else:
            existed = False
            before = 0
            added = 0
            session.sql(ddl_text).collect()

        # Upload the CSV write_outputs() already produced, then COPY INTO.
        stage_path = f"@{stage_name}/{table}.csv"
        with open(csv_path, "rb") as f:
            session.file.put_stream(f, stage_path, auto_compress=True, overwrite=True)

        col_list = ", ".join(needed_cols)
        session.sql(f"""
            COPY INTO {table} ({col_list})
            FROM {stage_path}.gz
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1 NULL_IF = (''))
            ON_ERROR = 'ABORT_STATEMENT'
        """).collect()

        after = _row_count(session, table)
        summary_lines.append(
            f"{table:<50} existed_before={existed!s:<5} cols_added={added:<3} "
            f"rows_before={before:<6} rows_after={after}"
        )

    return "\n".join(summary_lines)
$$;
