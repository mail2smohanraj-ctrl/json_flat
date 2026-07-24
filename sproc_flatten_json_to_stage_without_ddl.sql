-- sproc_flatten_json_to_stage.sql
--
-- Simplified version of FLATTEN_JSON_VERBATIM.
-- Flattens a JSON document into one or more tabular structures (root +
-- child tables for nested lists/objects, same logic as the original
-- flatten_json.py) and writes each one out as a CSV, then uploads those
-- CSVs to an internal stage.
--
-- Everything related to DDL generation (CREATE TABLE), type inference,
-- ALTER TABLE, and COPY INTO / table loading has been removed - this
-- procedure ONLY parses the JSON and produces CSV files in the stage.
--
-- Deploy:
--   snowsql -f sproc_flatten_json_to_stage.sql
--
-- Call:
--   CALL FLATTEN_JSON_TO_STAGE(
--     (SELECT raw_data::STRING FROM raw_sample LIMIT 1),  -- JSON text
--     'sample',                 -- root table name (used as filename prefix)
--     'sample.json',            -- source file label (lineage column)
--     'JSON_LOAD_STAGE'         -- stage to upload the generated CSVs to
--   );
--
-- Result: one sample.csv, sample__<child>.csv, ... per flattened table,
-- uploaded to @JSON_LOAD_STAGE (gzip-compressed, as .csv.gz).

CREATE OR REPLACE PROCEDURE FLATTEN_JSON_TO_STAGE(
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
import csv
import json
import os
import re
import tempfile
import uuid
from collections import OrderedDict, defaultdict

# ---------------------------------------------------------------------------
# Global state (per run, across nested objects so schemas merge within a call)
# ---------------------------------------------------------------------------
tables = defaultdict(list)                 # table_name -> list[row dict]
table_columns_order = defaultdict(list)    # table_name -> [col1, col2, ...] first-seen order
table_order = []                           # first-seen order = safe creation order


def _reset_state():
    """Snowflake may reuse a warm Python runtime across separate CALLs, so
    clear module-level state at the start of every call."""
    tables.clear()
    table_columns_order.clear()
    table_order.clear()


def sanitize_name(name):
    """Make a string safe to use as a table/column identifier."""
    name = re.sub(r"[^0-9a-zA-Z_]", "_", str(name)).strip("_")
    if not name:
        name = "col"
    if name[0].isdigit():
        name = f"c_{name}"
    return name.lower()


def register_row(table_name, row, parent_table):
    """Store a row and track column order."""
    if table_name not in table_order:
        table_order.append(table_name)

    for col in row:
        if col not in table_columns_order[table_name]:
            table_columns_order[table_name].append(col)

    tables[table_name].append(row)


def flatten_object(obj, table_name, parent_col=None, parent_id=None, source_file=None, parent_table=None):
    """Flatten one JSON object into `table_name`, recursing into nested
    lists/objects to populate child tables. Returns the row's surrogate id."""
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


def write_csvs(output_dir):
    """Write one CSV per flattened table. No DDL, no type inference -
    just the tabular data."""
    os.makedirs(output_dir, exist_ok=True)
    written = []

    for table in table_order:
        cols = table_columns_order[table]
        rows = tables[table]

        csv_path = os.path.join(output_dir, f"{table}.csv")
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            writer.writerow(cols)
            for row in rows:
                writer.writerow([row.get(c, "") if row.get(c, "") is not None else "" for c in cols])

        written.append((table, csv_path, len(rows), len(cols)))

    return written


def run(session, json_text, root_table_name, source_file_label, stage_name):
    # 1) Reset state left over from a prior CALL in a warm runtime.
    _reset_state()

    # 2) Parse the JSON text handed to the procedure.
    doc = json.loads(json_text)

    # 3) Flatten into one or more tables, in memory.
    base_table = sanitize_name(root_table_name)
    if isinstance(doc, dict):
        flatten_object(doc, base_table, source_file=source_file_label)
    elif isinstance(doc, list):
        for item in doc:
            flatten_object(item, base_table, source_file=source_file_label)
    else:
        return "Top-level JSON must be an object or array."

    # 4) Write each table to a CSV file.
    output_dir = tempfile.mkdtemp()
    written = write_csvs(output_dir)

    # 5) Upload each CSV to the stage. No table creation, no COPY INTO.
    session.sql(f"CREATE STAGE IF NOT EXISTS {stage_name}").collect()

    summary_lines = []
    for table, csv_path, row_count, col_count in written:
        stage_path = f"@{stage_name}/{table}.csv"
        with open(csv_path, "rb") as f:
            session.file.put_stream(f, stage_path, auto_compress=True, overwrite=True)
        summary_lines.append(
            f"{table:<50} rows={row_count:<6} cols={col_count:<3} -> {stage_path}.gz"
        )

    return "\n".join(summary_lines)
$$;
