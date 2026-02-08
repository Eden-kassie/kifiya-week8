with base as (
    select
        channel_name,
<<<<<<< HEAD
        min(message_date) as first_post_date,
        max(message_date) as last_post_date,
        count(*) as total_posts,
        avg(view_count)::numeric(18,2) as avg_views
=======
        min(message_ts)::date as first_post_date,
        max(message_ts)::date as last_post_date,
        count(*) as total_posts,
        avg(view_count)::numeric(12,2) as avg_views
>>>>>>> f514872 (final and updated)
    from {{ ref('stg_telegram_messages') }}
    group by 1
),

typed as (
    select
        channel_name,
        case
            when channel_name like '%pharma%' then 'Pharmaceutical'
            when channel_name like '%cosmetic%' or channel_name like '%lobelia%' then 'Cosmetics'
            else 'Medical'
        end as channel_type,
        first_post_date,
        last_post_date,
        total_posts,
        avg_views
    from base
)

select
    {{ dbt_utils.generate_surrogate_key(['channel_name']) }} as channel_key,
    channel_name,
    channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    avg_views
<<<<<<< HEAD
from typed;
=======
from typed
>>>>>>> f514872 (final and updated)
