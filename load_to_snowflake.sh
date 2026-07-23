#!/usr/bin/env bash
#
# load_to_snowflake.sh  (generic version)
#
# Works with ANY JSON shape - it doesn't hardcode table/column names.
# It relies entirely on what flatten_json.py discovered at runtime:
#   <output_dir>/table_order.txt        -- tables in safe creation order
#   <output_dir>/ddl/<table>.sql        -- CREATE TABLE statement
#   <output_dir>/staging/<table>.csv    -- data
#   <output_dir>/staging/<table>.columns -- exact CSV column order/names
#
# For each table:
#   1. Check INFORMATION_SCHEMA.TABLES -> does it already exist?
#      - No  -> run its ddl/<table>.sql to create it.
#      - Yes -> compare its existing columns (INFORMATION_SCHEMA.COLUMNS)
#               against the columns this run needs; ALTER TABLE ADD COLUMN
#               for anything new (handles schema drift across JSON files
#               that introduce new fields over time). Never drops columns.
#   2. Stage the CSV and COPY INTO using an explicit column list (safe
#      regardless of column ordering differences).
#   3. Verify row counts before/after.
# Finally prints a summary.
#
# Usage:
#   export SNOWSQL_ACCOUNT=... SNOWSQL_USER=... SNOWSQL_PWD=... \
#          SNOWSQL_ROLE=... SNOWSQL_WAREHOUSE=... SNOWSQL_DATABASE=... SNOWSQL_SCHEMA=...
#   ./load_to_snowflake.sh <output_dir> <input_json_file_or_dir> [more ...]
#
# <output_dir> is where flatten_json.py will write staging/ddl files, and
# where this script reads them back from - same directory used for both.

set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <output_dir> <input_json_file_or_dir> [more files/dirs ...]"
  exit 1
fi

OUTPUT_DIR="$1"; shift
INPUT_PATHS=("$@")

STAGE_NAME="JSON_LOAD_STAGE"

: "${SNOWSQL_ACCOUNT:?Set SNOWSQL_ACCOUNT}"
: "${SNOWSQL_USER:?Set SNOWSQL_USER}"
: "${SNOWSQL_ROLE:?Set SNOWSQL_ROLE}"
: "${SNOWSQL_WAREHOUSE:?Set SNOWSQL_WAREHOUSE}"
: "${SNOWSQL_DATABASE:?Set SNOWSQL_DATABASE}"
: "${SNOWSQL_SCHEMA:?Set SNOWSQL_SCHEMA}"
# SNOWSQL_PWD (or key-pair auth env vars) must also be set/exported by the caller.

SNOWSQL_CONN_ARGS=(
  -a "$SNOWSQL_ACCOUNT"
  -u "$SNOWSQL_USER"
  -r "$SNOWSQL_ROLE"
  -w "$SNOWSQL_WAREHOUSE"
  -d "$SNOWSQL_DATABASE"
  -s "$SNOWSQL_SCHEMA"
)

declare -A EXISTED_BEFORE
declare -A ROWS_BEFORE
declare -A ROWS_AFTER
declare -A COLS_ADDED

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
run_query() {
  local sql="$1"
  snowsql "${SNOWSQL_CONN_ARGS[@]}" \
    -o output_format=csv -o header=false -o timing=false -o friendly=false \
    -q "$sql"
}

run_script() {
  local file="$1"
  snowsql "${SNOWSQL_CONN_ARGS[@]}" \
    -o friendly=false -o timing=false \
    -f "$file"
}

table_exists() {
  local table_name="$1"
  local count
  count=$(run_query "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = UPPER('${SNOWSQL_SCHEMA}') AND TABLE_NAME = UPPER('${table_name}');" | tr -d '[:space:]')
  [[ "$count" -gt 0 ]]
}

row_count() {
  local table_name="$1"
  run_query "SELECT COUNT(*) FROM ${table_name};" | tr -d '[:space:]'
}

existing_columns() {
  # Returns existing column names, one per line, lowercase
  local table_name="$1"
  run_query "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = UPPER('${SNOWSQL_SCHEMA}') AND TABLE_NAME = UPPER('${table_name}') ORDER BY ORDINAL_POSITION;" \
    | tr '[:upper:]' '[:lower:]' | tr -d '[:space:]' | sed '/^$/d'
}

# Extract "<colname> <TYPE...>" for a given column from the generated DDL file
# so ALTER TABLE ADD COLUMN uses the same inferred type.
column_def_from_ddl() {
  local ddl_file="$1" col="$2"
  # Matches lines like:   colname TYPE(...),   or   colname TYPE
  grep -E "^\s*${col}\s" "$ddl_file" | head -n1 \
    | sed -E 's/^\s*//; s/,\s*$//' \
    | sed -E 's/ PRIMARY KEY//; s/ REFERENCES [A-Za-z0-9_]+\([A-Za-z0-9_]+\)//'
}

# ---------------------------------------------------------------------------
# 0. Flatten the JSON (works for any shape, any number of files)
# ---------------------------------------------------------------------------
echo "== Step 0: Flattening input into ${OUTPUT_DIR}/ =="
python3 flatten_json.py "$OUTPUT_DIR" "${INPUT_PATHS[@]}"
echo

TABLE_ORDER_FILE="${OUTPUT_DIR}/table_order.txt"
if [[ ! -f "$TABLE_ORDER_FILE" ]]; then
  echo "ERROR: ${TABLE_ORDER_FILE} not found - flattening may have failed."
  exit 1
fi
mapfile -t TABLES < "$TABLE_ORDER_FILE"

# ---------------------------------------------------------------------------
# 1. Create the internal stage (idempotent)
# ---------------------------------------------------------------------------
echo "== Step 1: Ensuring internal stage ${STAGE_NAME} exists =="
run_query "CREATE STAGE IF NOT EXISTS ${STAGE_NAME};"
echo

# ---------------------------------------------------------------------------
# 2. For each discovered table: check -> create or ALTER -> stage -> load -> verify
# ---------------------------------------------------------------------------
for table in "${TABLES[@]}"; do
  [[ -z "$table" ]] && continue
  echo "== Processing table: ${table} =="

  ddl_file="${OUTPUT_DIR}/ddl/${table}.sql"
  columns_file="${OUTPUT_DIR}/staging/${table}.columns"
  csv_file="${OUTPUT_DIR}/staging/${table}.csv"
  needed_cols=$(cat "$columns_file")
  COLS_ADDED[$table]=0

  if table_exists "$table"; then
    EXISTED_BEFORE[$table]="YES"
    ROWS_BEFORE[$table]=$(row_count "$table")
    echo "  Exists already (rows: ${ROWS_BEFORE[$table]}). Checking for new columns..."

    existing_cols_list=$(existing_columns "$table")
    IFS=',' read -ra needed_cols_arr <<< "$needed_cols"
    for col in "${needed_cols_arr[@]}"; do
      if ! grep -qx "$col" <<< "$existing_cols_list"; then
        col_def=$(column_def_from_ddl "$ddl_file" "$col")
        if [[ -n "$col_def" ]]; then
          echo "    New column detected: ${col} -> ALTER TABLE ${table} ADD COLUMN ${col_def};"
          run_query "ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS ${col_def};"
          COLS_ADDED[$table]=$(( ${COLS_ADDED[$table]} + 1 ))
        fi
      fi
    done
    echo "  NOTE: existing table will be APPENDED to. Truncate manually first for a full reload."
  else
    EXISTED_BEFORE[$table]="NO"
    ROWS_BEFORE[$table]=0
    echo "  Does not exist. Creating from ${ddl_file} ..."
    run_script "$ddl_file"
  fi

  echo "  Staging ${csv_file} -> @${STAGE_NAME}"
  run_query "PUT file://$(pwd)/${csv_file} @${STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"

  echo "  Loading into ${table} via COPY INTO (explicit column list) ..."
  run_query "
    COPY INTO ${table} (${needed_cols})
    FROM @${STAGE_NAME}/${table}.csv.gz
    FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1 NULL_IF = (''))
    ON_ERROR = 'ABORT_STATEMENT';
  "

  ROWS_AFTER[$table]=$(row_count "$table")
  echo "  Rows after load: ${ROWS_AFTER[$table]}"
  echo
done

# ---------------------------------------------------------------------------
# 3. Summary
# ---------------------------------------------------------------------------
echo "== Summary =="
printf "%-55s %-14s %-12s %-12s %-10s\n" "Table" "ExistedBefore" "RowsBefore" "RowsAfter" "ColsAdded"
for table in "${TABLES[@]}"; do
  [[ -z "$table" ]] && continue
  printf "%-55s %-14s %-12s %-12s %-10s\n" "$table" "${EXISTED_BEFORE[$table]}" "${ROWS_BEFORE[$table]}" "${ROWS_AFTER[$table]}" "${COLS_ADDED[$table]}"
done
