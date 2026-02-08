<<<<<<< HEAD
{{ config(materialized='table') }}

with dates as (
    {{ dbt_date.get_date_dimension("2019-01-01", "2032-12-31") }}
)

select
    cast(to_char(date_day, 'YYYYMMDD') as int) as date_key,
    date_day as full_date,
    day_of_week,
    day_name,
    week_of_year,
    month_of_year as month,
    month_name,
    quarter_of_year as quarter,
    year_number as year,
    case when day_of_week in (6, 7) then true else false end as is_weekend
from dates;
=======
with dates as (
    select distinct
        message_ts::date as full_date
    from {{ ref('stg_telegram_messages') }}
)

select
    (extract(year from full_date)::int * 10000
     + extract(month from full_date)::int * 100
     + extract(day from full_date)::int) as date_key,
    full_date,
    extract(isodow from full_date)::int as day_of_week,
    trim(to_char(full_date, 'Day')) as day_name,
    extract(week from full_date)::int as week_of_year,
    extract(month from full_date)::int as month,
    trim(to_char(full_date, 'Month')) as month_name,
    extract(quarter from full_date)::int as quarter,
    extract(year from full_date)::int as year,
    case when extract(isodow from full_date) in (6,7) then true else false end as is_weekend
from dates
>>>>>>> f514872 (final and updated)
