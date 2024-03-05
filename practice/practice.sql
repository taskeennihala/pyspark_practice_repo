USE demo1

--check for duplicates in the Taxi Zone file

SELECT
    LocationID,
     COUNT(1) AS number_of_records
FROM
    OPENROWSET(
        BULK 'https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRST_ROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    )
    WITH(
        LocationID SMALLINT 1,
        Borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15)4
    )
AS [result]
GROUP BY LocationID
HAVING COUNT(1) > 1;

SELECT
     Borough,
     COUNT(1) AS number_of_records
FROM
    OPENROWSET(
        BULK 'https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/taxi_zone.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIRST_ROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    )
    WITH(
        LocationID SMALLINT 1,
        Borough VARCHAR(15) 2,
        zone VARCHAR(50) 3,
        service_zone VARCHAR(15)4
    )
AS [result]
GROUP BY Borough
HAVING COUNT(1) > 1;


USE demo1

--identify any data quality issues in trip total amount

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        FORMAT = 'PARQUET',
        Data_Source = 'nyc_practice1'
    ) AS [result]

SELECT
    MIN(total_amount)AS min_total_amt,
    MAX(total_amount)AS max_total_amt,
    AVG(total_amount)AS avg_total_amt,
    COUNT(1)as total_num_of_records,
    COUNT(total_amount) AS not_null_total_number_of_records
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        FORMAT = 'PARQUET',
        Data_Source = 'nyc_practice1'
    ) AS [result]

USE demo1

--identify number of trip made from each brough

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        FORMAT = 'PARQUET',
        Data_Source = 'nyc_practice1'
    ) AS [result]
WHERE PULocationID is NULL

SELECT taxi_zone.*,trip_data.*
   FROM OPENROWSET(
                    BULK 'trip_data_green_parquet/year=2020/month=01/',
                    FORMAT = 'PARQUET',
                    Data_Source = 'nyc_practice1'
                ) AS trip_data
        JOIN
        OPENROWSET(
                    BULK 'https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/taxi_zone.csv',
                    FORMAT = 'CSV',
                    PARSER_VERSION = '2.0',
                    FIRST_ROW = 2
                )
                WITH (
                    location_id SMALLINT 1,
                    borough VARCHAR(15) 2,
                    zone VARCHAR(50) 3,
                    service_zone VARCHAR(15)4
                )AS taxi_zone
        ON trip_data.PULocationID = taxi_zone.location_id

-- count of borough

SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        FORMAT = 'PARQUET',
        Data_Source = 'nyc_practice1'
    ) AS [result]
WHERE PULocationID is NULL

SELECT taxi_zone.borough, COUNT(1) AS number_of_trips
  FROM OPENROWSET(
                    BULK 'trip_data_green_parquet/year=2020/month=01/',
                    FORMAT = 'PARQUET',
                    Data_Source = 'nyc_practice1'
                ) AS trip_data
        JOIN
        OPENROWSET(
                    BULK 'https://sftpdemoo.dfs.core.windows.net/nyc-taxidata/raw/taxi_zone.csv',
                    FORMAT = 'CSV',
                    PARSER_VERSION = '2.0',
                    FIRST_ROW = 2
                )
                WITH (
                    location_id SMALLINT 1,
                    borough VARCHAR(15) 2,
                    zone VARCHAR(50) 3,
                    service_zone VARCHAR(15)4
                )AS taxi_zone
        ON trip_data.PULocationID = taxi_zone.location_id
GROUP BY taxi_zone.borough
ORDER BY number_of_trips;

--Number of trips made by duration in hours

SELECT TOP 100 
    lpep_pickup_datetime,
    lpep_dropoff_datetime,
    DATEDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) / 60 AS from_hour,
    (DATEDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) / 60) + 1 AS to_hour
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        FORMAT = 'PARQUET',
        Data_Source = 'nyc_practice1'
    ) AS [result]

-- count 

SELECT TOP 100 
    DATEDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) / 60 AS from_hour,
    (DATEDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) / 60) + 1 AS to_hour,
    COUNT(1)AS number_of_trips
FROM
    OPENROWSET(
        BULK 'trip_data_green_parquet/year=2020/month=01/',
        FORMAT = 'PARQUET',
        Data_Source = 'nyc_practice1'
    ) AS trip_data
GROUP BY DATEDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) / 60,
(DATEDIFF(MINUTE, lpep_pickup_datetime, lpep_dropoff_datetime) / 60) + 1 
ORDER BY from_hour, to_hour;