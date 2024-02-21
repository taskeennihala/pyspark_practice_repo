SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/taxi_zone.csv',
        FORMAT = 'CSV',
PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) AS [result]

-- Explains the data types of the column
EXEC sp_describe_first_result_set N'SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/taxi_zone.csv'',
        FORMAT = ''CSV'',
PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE
    ) AS [result]'

--Max of columns
SELECT
    MAX(LEN(LocationId))AS len_locationId,
    MAX(LEN(Borough))AS len_Borough,
    MAX(LEN(Zone))AS len_Zone,
    MAX(LEN(service_zone))AS len_service_zone
FROM
    OPENROWSET(
        BULK 'https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/taxi_zone.csv',
        FORMAT = 'CSV',
PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) AS [result]

--use with clause to provide explicit data types
SELECT
     *
FROM
    OPENROWSET(
        BULK 'https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/taxi_zone.csv',
        FORMAT = 'CSV',
PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    ) 
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(15),
        zone VARCHAR(50),
        service_zone VARCHAR(15)
    )AS [result]

EXEC sp_describe_first_result_set N'SELECT
     *
FROM
    OPENROWSET(
        BULK ''https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/taxi_zone.csv'',
        FORMAT = ''CSV'',
PARSER_VERSION = ''2.0'',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = '','',
        ROWTERMINATOR = ''\n''
    ) 
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(15),
        zone VARCHAR(50),
        service_zone VARCHAR(15)
    )AS [result]'

SELECT name, collation_name from sys.databases;

--specify UTF-8 collation for VARCHAR columns
SELECT
     *
FROM
    OPENROWSET(
        BULK 'https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/taxi_zone.csv',
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

--select only subset of columns
SELECT
     *
FROM
    OPENROWSET(
        BULK 'https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/taxi_zone.csv',
        FORMAT = 'CSV',
PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    )
    WITH(
        LocationID SMALLINT,
        Borough VARCHAR(15),
        zone VARCHAR(50),
        service_zone VARCHAR(15)
    )
AS [result]