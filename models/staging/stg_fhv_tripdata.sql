{{ config(materialized='view') }}

with tripdata as 
(
  select
        cast(dispatching_base_num as string) as dispatching_base_num
        ,cast(pickup_datetime as timestamp) as pickup_datetime
        ,cast(dropOff_datetime as timestamp) as dropOff_datetime
        ,PUlocationID
        ,DOlocationID
        ,cast(SR_Flag as string) as SR_Flag
        ,cast(Affiliated_base_number as string) as Affiliated_base_number
  from {{ source('staging','fhv_materialized_table') }}
  where extract(year from cast(pickup_datetime as date)) = 2019
)
select
    -- identifiers (timestamp, integer,string)
     dispatching_base_num
    ,pickup_datetime
    ,dropOff_datetime
    ,PUlocationID
    ,DOlocationID
    ,SR_Flag
    ,Affiliated_base_number 
from tripdata



-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}