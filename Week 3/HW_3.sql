CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.fhv_2019_trips_external`
-- (
--     dispatching_base_num STRING,
--     pickup_datetime datetime,
--     dropOff_datetime datetime,
--     PUlocationID FLOAT64,	
--     DOlocationID FLOAT64,	
--     SR_Flag	FLOAT64,
--     Affiliated_base_number STRING

--  )
OPTIONS (
    format = 'PARQUET',
    uris = ['gs://dtc_data_lake_de-zoomcamp-376202/data\\fhv_tripdata_2019-*.parquet']
)
;






select count(*)
from `trips_data_all.fhv_2019_trips`
where PUlocationID = 'nan' and DOlocationID = 'nan'



CREATE OR REPLACE TABLE `trips_data_all.fhv_2019_trips_partitioned_clustered`
PARTITION BY date(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * from `trips_data_all.fhv_2019_trips`;



SELECT distinct Affiliated_base_number
from `trips_data_all.fhv_2019_trips`
where date(pickup_datetime) between '2019-03-01' and '2019-03-21'

SELECT distinct Affiliated_base_number
from `trips_data_all.fhv_2019_trips_partitioned_clustered`
where date(pickup_datetime) between '2019-03-01' and '2019-03-21'