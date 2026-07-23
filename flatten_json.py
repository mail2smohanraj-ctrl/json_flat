#!/usr/bin/env python3
"""
flatten_json.py  (generic version)

Flattens ANY JSON file(s) - arbitrary field names, arbitrary nesting depth,
arbitrary number of nested lists/objects - into a normalized set of
relational tables, ready to load into Snowflake.

No hardcoded field/table names. Works for the sample file AND for
real-world JSON with a completely different shape. Can process one file
or many files in a single run (schemas are merged/unioned automatically).

Rules used to flatten:
  - A JSON object's SCALAR fields become columns on its table.
  - A nested OBJECT field is flattened into the SAME row using a
    "parentfield_childfield" column prefix (no new table - it's a 1:1
    relationship, so no join is needed).
  - A LIST of scalars becomes a CHILD table: "<table>__<field>"
    with columns (<table>__<field>_id, <table>_id, value).
  - A LIST of objects becomes a CHILD table: "<table>__<field>"
    with one row per object (recursively flattened the same way),
    plus a surrogate id and a foreign key back to the parent row.
  - Every row gets a UUID surrogate primary key, stable across files/runs
    (no collisions when appending or merging multiple JSON files).
  - Every row also gets a "_source_file" column for lineage.
  - Column types are inferred from observed values; a column with mixed
    incompatible types across rows safely falls back to VARCHAR.

Usage:
    python3 flatten_json.py <output_dir> <input_json_file> [<input_json_file> ...]
    python3 flatten_json.py <output_dir> <input_json_directory>

Output (written under <output_dir>):
    staging/<table>.csv        -- data for each discovered table
    staging/<table>.columns    -- comma-separated column list (CSV column order)
    ddl/<table>.sql            -- CREATE TABLE statement for each table
    table_order.txt            -- safe creation order (parents before children)
"""

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


def collect_json_files(paths):
    files = []
    for p in paths:
        if os.path.isdir(p):
            for fn in sorted(os.listdir(p)):
                if fn.lower().endswith(".json"):
                    files.append(os.path.join(p, fn))
        else:
            files.append(p)
    return files


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <output_dir> <input_json_file_or_dir> [more files/dirs ...]")
        sys.exit(1)

    output_dir = sys.argv[1]
    input_paths = sys.argv[2:]
    json_files = collect_json_files(input_paths)

    if not json_files:
        print("No .json files found in the given input path(s).")
        sys.exit(1)

    for path in json_files:
        base_table = sanitize_name(os.path.splitext(os.path.basename(path))[0])
        print(f"Parsing {path} -> root table '{base_table}'")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            for item in data:
                flatten_object(item, base_table, source_file=os.path.basename(path))
        elif isinstance(data, dict):
            flatten_object(data, base_table, source_file=os.path.basename(path))
        else:
            print(f"  Skipping {path}: top-level JSON must be an object or array.")

    print("\nDiscovered tables:")
    write_outputs(output_dir)
    print(f"\nDone. {len(table_order)} tables written under '{output_dir}'.")


if __name__ == "__main__":
    main()
