DROP DATABASE IF EXISTS nycitibike_database;
CREATE DATABASE nycitibike_database;
-- CREATE WAREHOUSE nycitibike_warehouse;
CREATE SCHEMA nycitibike_schema;NYCITIBIKE_DATABASE
// Create Table
-- TRUNCATE TABLE nycitibike_database.nycitibike_schema.nycitibike_table;
CREATE OR REPLACE TABLE nycitibike_database.nycitibike_schema.nycitibike_table (
last_reported TIMESTAMP_NTZ,
num_docks_disabled INT,
num_ebikes_available INT,
station_id STRING,
num_bikes_disabled INT,
num_bikes_available INT,
num_docks_available INT,
id INT,
name STRING,
lat FLOAT,
lng FLOAT,
free_docks INT,
timestamp TIMESTAMP_NTZ,
normal_bike INT,
e_bike INT
);
SELECT *
FROM nycitibike_database.nycitibike_schema.nycitibike_table LIMIT 10;

SELECT COUNT(*) FROM nycitibike_database.nycitibike_schema.nycitibike_table
-- DESC TABLE nycitibike_database.nycitibike_schema.nycitibike_table;


// Create file format object
CREATE SCHEMA file_format_schema;
CREATE OR REPLACE file format nycitibike_database.file_format_schema.format_csv
    type = 'CSV'
    field_delimiter = ','
    RECORD_DELIMITER = '\n'
    skip_header = 1
    -- error_on_column_count_mismatch = FALSE;
    
// Create staging schema
CREATE SCHEMA external_stage_schema;
// Create staging
-- DROP STAGE nycitibike_database.external_stage_schema.nycitibike_ext_stage_yml;
CREATE OR REPLACE STAGE nycitibike_database.external_stage_schema.nycitibike_ext_stage_yml 
    url="s3://nycitibike-transform-zone-yml/"
    credentials=(aws_key_id=''
    aws_secret_key='')
    FILE_FORMAT = nycitibike_database.file_format_schema.format_csv;

list @nycitibike_database.external_stage_schema.nycitibike_ext_stage_yml;

// Create schema for snowpipe
-- DROP SCHEMA nycitibike_database.snowpipe_schema;
CREATE OR REPLACE SCHEMA nycitibike_database.snowpipe_schema;

// Create Pipe
CREATE OR REPLACE PIPE nycitibike_database.snowpipe_schema.nycitibike_snowpipe
auto_ingest = TRUE
AS 
COPY INTO nycitibike_database.nycitibike_schema.nycitibike_table
FROM @nycitibike_database.external_stage_schema.nycitibike_ext_stage_yml;

DESC PIPE nycitibike_database.snowpipe_schema.nycitibike_snowpipe;