--created external data source
CREATE EXTERNAL DATA SOURCE nyc_taxidata_2
with(
    LOCATION = 'https://sftpdemoo.dfs.core.windows.net/'
)

SELECT
     *
FROM
    OPENROWSET(
        BULK 'raw/taxi_zone.csv',
        DATA_SOURCE = 'nyc_taxi_data',
        FORMAT = 'CSV',
PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(15)COLLATE Latin1_General_100_CI_AI_SC_UTF8,
        zone VARCHAR(50)COLLATE Latin1_General_100_CI_AI_SC_UTF8,
        service_zone VARCHAR(15)COLLATE Latin1_General_100_CI_AI_SC_UTF8
    )AS [result]

CREATE EXTERNAL DATA SOURCE nyc_taxi_data_raw
with(
    LOCATION = 'https://sftpdemoo.dfs.core.windows.net/'
)

SELECT
     *
FROM
    OPENROWSET(
        BULK 'taxi_zone.csv',
        DATA_SOURCE = 'nyc_practice',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(15)COLLATE Latin1_General_100_CI_AI_SC_UTF8,
        zone VARCHAR(50)COLLATE Latin1_General_100_CI_AI_SC_UTF8,
        service_zone VARCHAR(15)COLLATE Latin1_General_100_CI_AI_SC_UTF8
    )AS [result]

SELECT name location FROM sys.external_data_sources

USE demo1
--vendor file
SELECT *
  FROM OPENROWSET(
    BULK 'vendor_unquoted.csv',
    DATA_SOURCE = 'nyc_practice1',
    FORMAT = 'csv',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE
  )AS vendor;

  SELECT *
  FROM OPENROWSET(
    BULK 'vendor_escaped.csv',
    DATA_SOURCE = 'nyc_practice1',
    FORMAT = 'csv',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    ESCAPECHAR = '\\'
  )AS vendor;

SELECT *
  FROM OPENROWSET(
    BULK 'vendor.csv',
    DATA_SOURCE = 'nyc_practice1',
    FORMAT = 'csv',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDQUOTE = '"'
  )AS vendor;

--Trip file
SELECT * 
  FROM OPENROWSET(
    BULK 'trip_type.tsv',
    DATA_SOURCE = 'nyc_practice1',
    FORMAT = 'csv',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE,
    FIELDTERMINATOR = '\t' 
  )AS trip_type;

--calendar file
USE demo1

SELECT * 
  FROM OPENROWSET(
    BULK 'calendar.csv',
    DATA_SOURCE = 'nyc_practice1',
    FORMAT = 'csv',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE 
  )AS cal;
EXEC sp_describe_first_result_set N'SELECT * 
  FROM OPENROWSET(
    BULK ''calendar.csv'',
    DATA_SOURCE = ''nyc_practice1'',
    FORMAT = ''csv'',
    PARSER_VERSION = ''2.0'',
    HEADER_ROW = TRUE 
  )AS cal;
'
SELECT * 
  FROM OPENROWSET(
    BULK 'calendar.csv',
    DATA_SOURCE = 'nyc_practice1',
    FORMAT = 'csv',
    PARSER_VERSION = '2.0',
    HEADER_ROW = TRUE 
  )
  WITH(
    date_key     INT,
    date         DATE,
    year         SMALLINT,
    month        TINYINT,
    day          TINYINT,
    day_name     VARCHAR(10),
    day_of_year  SMALLINT,
    week_of_month TINYINT,
    week_of_year  TINYINT,
    month_name    VARCHAR(10),
    year_month    INT,
    year_week     INT
  )AS cal;

