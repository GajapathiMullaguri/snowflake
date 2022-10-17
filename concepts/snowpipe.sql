CREATE OR REPLACE TABLE annual_enterprice_survey(
`YEAR` int ,
Industry_aggregation_NZSIOC string,
Industry_code_NZSIOC string,
Industry_name_NZSIOC string,
Units string,
Variable_code string,
Variable_name string,
Variable_category string,
Value string,
Industry_code_ANZSIC06 string
)

DESC TABLE annual_enterprice_survey;

CREATE OR REPLACE FILE FORMAT annual_survey_csv
type = csv
field_delimiter=','
skip_header=1
empty_field_as_null=TRUE
field_optionally_enclosed_by='"';

DESC FILE FORMAT annual_survey_csv;

alter storage integration s3_int SET storage_allowed_locations=('s3://026823805161-airflow/','s3://026823805161-snowflake/');

DESC storage integration s3_int;

CREATE OR REPLACE STAGE annual_survey_stage
url='s3://026823805161-snowflake/snowpipe/employees_data/'
storage_integration = s3_int
file_format = annual_survey_csv;

LIST @annual_survey_stage;

CREATE OR REPLACE pipe annual_survey_pipe
auto_ingest = TRUE
AS
COPY INTO annual_enterprice_survey
FROM @annual_survey_stage;

DESC pipe annual_survey_pipe;

--Error Handling
alter pipe annual_survey_pipe refresh;

SELECT count(*) FROM annual_enterprice_survey;

SELECT SYSTEM$pipe_status('annual_survey_pipe');

SELECT *FROM TABLE(validate_pipe_load(
pipe_name => 'annual_survey_pipe',
start_time => Dateadd(HOUR,-2,current_timestamp())
));

SELECT *FROM table(INFORMATION_SCHEMA.copy_history(
table_name => 'annual_enterprice_survey',
start_time => Dateadd(HOUR,-2,current_timestamp())
));