{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    row_number() over(partition by dispatching_base_num, pickup_datetime, dropoff_datetime) as rn
  from {{ source('staging','fhv_tripdata_2019') }}
)
select
    -- identifiers
    {{ dbt_utils.surrogate_key(['dispatching_base_num', 'pickup_datetime', 'dropoff_datetime']) }} as tripid,
    cast(dispatching_base_num as dispatching_base_num,
    cast(affiliated_base_number as affiliated_base_number,
    cast(pulocationid as pickup_locationid,
    cast(dolocationid as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- trip info
    cast(SR_Flag as integer) as is_shared
    
   
from tripdata
where rn = 1


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}