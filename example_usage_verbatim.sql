-- example_usage_verbatim.sql
--
-- Deploy once:
--   snowsql -f sproc_flatten_json_verbatim.sql
--
-- Then call it, feeding JSON as a STRING - either straight from a landing
-- table's VARIANT column (cast to STRING), or literally, if you already
-- have the text in hand.

-- Option A: JSON already landed in a VARIANT column
CREATE TABLE IF NOT EXISTS raw_sample (
    raw_id       VARCHAR(64) DEFAULT UUID_STRING(),
    raw_data     VARIANT,
    _source_file VARCHAR(1024),
    _loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
CREATE STAGE IF NOT EXISTS JSON_LOAD_STAGE;
-- (PUT + COPY INTO raw_sample as shown in earlier examples)

CALL FLATTEN_JSON_VERBATIM(
    (SELECT raw_data::STRING FROM raw_sample WHERE _source_file = 'sample.json' LIMIT 1),
    'sample',            -- root table name
    'sample.json',        -- source file label (lineage)
    'JSON_LOAD_STAGE'     -- stage used to hold the generated CSVs
);

-- Option B: pass JSON text directly (e.g. from an application, no landing
-- table needed at all)
-- CALL FLATTEN_JSON_VERBATIM('{"a": 1, "b": [1,2,3]}', 'my_doc', 'inline', 'JSON_LOAD_STAGE');

-- See what it created
SHOW TABLES LIKE 'sample%';
SELECT * FROM sample;
SELECT * FROM sample__departments__teams__members;
