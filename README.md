# Generic JSON → Snowflake Loader

Handles **any** JSON shape - arbitrary fields, arbitrary nesting depth,
arbitrary number of nested lists - and **any number of JSON files** in one
run. Nothing is hardcoded to the sample data; table/column names, types,
and relationships are all derived from the JSON itself at runtime.

## How it works

### `flatten_json.py`
For every JSON file given (a single file, several files, or a whole
directory):
- The root of the file becomes a table named after the file
  (`orders.json` → table `orders`).
- **Scalar fields** → columns on their table.
- **Nested object** (`"location": {"city": ..., "geo": {"lat":...}}`) →
  flattened into the *same* row as prefixed columns
  (`location_city`, `location_geo_lat`, ...) — no extra table, since it's
  a 1:1 relationship.
- **List of scalars** (`"tags": ["a","b"]`) → its own child table
  `<table>__tags` with one row per value.
- **List of objects** (`"reviews": [{...}, {...}]`) → its own child table
  `<table>__reviews`, recursively flattened the same way, with a foreign
  key back to the parent row.
- Every row gets a **UUID** surrogate primary key (not an auto-incrementing
  int), so re-running the pipeline or merging multiple files never
  collides on IDs.
- Every row also gets a `_source_file` column for lineage.
- Column **types are inferred** from the actual values seen (`NUMBER`,
  `FLOAT`, `BOOLEAN`, or `VARCHAR(n)` sized to the longest value). Columns
  with mixed/incompatible types safely fall back to `VARCHAR`.
- If two files each contribute rows to what becomes the same table (e.g.
  same shape processed twice) or one file's objects have inconsistent
  fields across items, columns are **unioned** automatically and missing
  values are left blank (loaded as `NULL`).

Output, all under one `<output_dir>`:
```
staging/<table>.csv        the data
staging/<table>.columns    exact column list/order for that CSV
ddl/<table>.sql            CREATE TABLE statement
table_order.txt            tables listed parents-before-children
```

### `load_to_snowflake.sh`
Reads whatever `flatten_json.py` produced — it never hardcodes a table
list — and for each table, in dependency order:
1. **Checks existence** via `INFORMATION_SCHEMA.TABLES`.
2. **If missing** → runs the generated `CREATE TABLE`.
3. **If it already exists** → compares its current columns
   (`INFORMATION_SCHEMA.COLUMNS`) against what this run needs, and runs
   `ALTER TABLE ... ADD COLUMN` for anything new. This is what lets the
   same target tables absorb JSON files that introduce new fields later,
   without breaking. Existing data and columns are never dropped.
4. Stages the CSV (`PUT`) and loads it with `COPY INTO` using an explicit
   column list (safe regardless of column ordering).
5. Verifies row counts before/after.
6. Prints a summary table (existed-before, rows before/after, columns
   added) for every table processed.

## Setup
```bash
export SNOWSQL_ACCOUNT="myorg-myaccount"
export SNOWSQL_USER="my_user"
export SNOWSQL_PWD="my_password"        # or configure key-pair auth instead
export SNOWSQL_ROLE="MY_ROLE"
export SNOWSQL_WAREHOUSE="MY_WH"
export SNOWSQL_DATABASE="MY_DB"
export SNOWSQL_SCHEMA="MY_SCHEMA"
chmod +x load_to_snowflake.sh
```

## Usage

Single file:
```bash
./load_to_snowflake.sh run_output sample_input/sample_multilevel.json
```

Multiple files in one run (schemas merge automatically):
```bash
./load_to_snowflake.sh run_output sample_input/sample_multilevel.json sample_input/store_inventory.json
```

An entire directory of JSON files:
```bash
./load_to_snowflake.sh run_output sample_input/
```

Just the flattening step, without touching Snowflake (useful to preview
the schema it will create):
```bash
python3 flatten_json.py preview_output sample_input/
cat preview_output/table_order.txt
cat preview_output/ddl/*.sql
```

## Included test files
Two structurally different JSON files are included under `sample_input/`
to demonstrate the generic behavior:
- `sample_multilevel.json` — departments/teams/members/skills and
  projects/tasks/subtasks/steps (mixed string and object subtasks)
- `store_inventory.json` — a completely different shape (nested
  `location.geo.lat/lng`, `categories`, `inventory` with `tags` and
  `reviews`)

Running the flattener against both at once produces 14 independent,
correctly-related tables with no naming collisions.

## Things to know before running against production data
- **Re-runs append** by default; the script warns rather than truncating.
  If you want idempotent re-loads, truncate the affected tables yourself
  first, or add a dedup step (e.g. `MERGE` on `_source_file` + natural
  key) on top of this.
- **VARCHAR sizing** is based on the longest value seen *in this run*.
  If a later file has a much longer value for the same column, the
  `ALTER TABLE ADD COLUMN` logic won't widen an existing column
  automatically — only brand-new columns are added. You'd want to extend
  the script with an `ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE`
  step if this matters for your data.
- **Very large files**: this loads the whole JSON into memory with the
  Python `json` module before flattening. For gigabyte-scale files,
  consider streaming (`ijson`) instead.
