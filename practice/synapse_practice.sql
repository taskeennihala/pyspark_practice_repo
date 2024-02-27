USE demo1

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=2020/month=01/green_tripdata_2020-01.csv',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
--select data from a folder
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=2020/month=01/*.csv',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
--select data from subfolders
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=2020/**',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]

--get data from more than one file
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ('trip_data_green_csv/year=2020/month=01/*.csv',
        'trip_data_green_csv/year=2020/month=02/*.csv'),
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
--using more than 1 wildcard character 
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
--file metadata function filename()
SELECT
    TOP 100 
    result.filename() AS file_name,
    result.*
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]

SELECT 
    result.filename() AS file_name,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
GROUP BY result.filename()
ORDER BY result.filename();
-- limit data using filename()
SELECT 
    result.filename() AS file_name,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
WHERE result.filename() IN ('green_tripdata_2020-01.csv','green_tripdata_2021-01.csv')
GROUP BY result.filename()
ORDER BY result.filename();
--use filepath function
SELECT 
    result.filename() AS file_name,
    result.filepath()AS file_path,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
WHERE result.filename() IN ('green_tripdata_2020-01.csv','green_tripdata_2021-01.csv')
GROUP BY result.filename(),result.filepath()
ORDER BY result.filename(),result.filepath();

--based on month year dynamically
SELECT 
    result.filename() AS file_name,
    result.filepath(1) AS YEAR,
    result.filepath(2) AS MONTH,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
WHERE result.filename() IN ('green_tripdata_2020-01.csv','green_tripdata_2021-01.csv')
GROUP BY result.filename(),result.filepath(1), result.filepath(2)
ORDER BY result.filename(),result.filepath(1), result.filepath(2);
--get the trip count each of the years and months
SELECT 
    result.filepath(1) AS YEAR,
    result.filepath(2) AS MONTH,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
GROUP BY result.filename(),result.filepath(1), result.filepath(2)
ORDER BY result.filename(),result.filepath(1), result.filepath(2);
--use file path in where caluse(to get data related to particular month)
SELECT 
    result.filepath(1) AS YEAR,
    result.filepath(2) AS MONTH,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_csv/year=*/month=*/*.csv',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE
    ) AS [result]
WHERE result.filepath(1) = '2020'
  AND result.filepath(2) IN ('06','07','08')
GROUP BY result.filename(),result.filepath(1), result.filepath(2)
ORDER BY result.filename(),result.filepath(1), result.filepath(2);


SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/trip_data_green_parquet/year=2020/month=01/part-00000-tid-6133789922049958496-2e489315-890a-4453-ae93-a104be9a6f06-106-1-c000.snappy.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/trip_data_green_parquet/year=2020/month=01/',
        FORMAT = 'PARQUET',
        DATA_SOURCE = 'nyc_practice1'
    ) AS [result]
--identify the  data type
EXEC sp_describe_first_result_set N'
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK ''trip_data_green_parquet/year=2020/month=01/'',
        FORMAT = ''PARQUET'',
        DATA_SOURCE = ''nyc_practice1''
    ) AS [result]'

/*
Assignment
1) query from folders using wildcard characters
2) use filename function
3) query from subfolders
4) use filepath function to select only from certain partition
*/
-- Assignment :
--1) query from folders using wildcard characters
SELECT TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
        FORMAT = 'PARQUET',
        DATA_SOURCE = 'nyc_practice1'
    ) AS trip_data

--2) use filename function
SELECT TOP 100 
    trip_data.filename() AS file_name,
    trip_data.*
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/*.parquet',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'parquet'
    ) AS trip_data

--3) query from subfolders
SELECT TOP 100 
    trip_data.filepath() AS file_path,
    trip_data.*
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/**',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'parquet'
    ) AS trip_data

--4) use filepath function to select only from certain partition
SELECT 
    trip_data.filepath(1) AS YEAR,
    trip_data.filepath(2) AS MONTH,
    COUNT(1) AS record_count
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=*/month=*/*.parquet',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'parquet'
    ) AS trip_data
WHERE trip_data.filepath(1) = '2020'
  AND trip_data.filepath(2) IN ('06','07','08')
GROUP BY trip_data.filepath(1), trip_data.filepath(2)
ORDER BY trip_data.filepath(1), trip_data.filepath(2);

-- delta files
use demo1

SELECT TOP 100 
       *
    FROM OPENROWSET(
        BULK 'trip_data_green_delta/',
        DATA_SOURCE = 'nyc_practice1',
        FORMAT = 'DELTA'
    )AS trip_data