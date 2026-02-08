with msg as (
<<<<<<< HEAD
    select * from {{ ref('stg_telegram_messages') }}
=======
    select
        message_id,
        channel_name,
        message_ts::date as full_date,
        message_text,
        message_length,
        view_count,
        forward_count,
        has_image
    from {{ ref('stg_telegram_messages') }}
>>>>>>> f514872 (final and updated)
),

ch as (
    select channel_key, channel_name
    from {{ ref('dim_channels') }}
<<<<<<< HEAD
=======
),

dt as (
    select date_key, full_date
    from {{ ref('dim_dates') }}
>>>>>>> f514872 (final and updated)
)

select
    msg.message_id,
    ch.channel_key,
<<<<<<< HEAD
    cast(to_char(msg.message_date, 'YYYYMMDD') as int) as date_key,
=======
    dt.date_key,
>>>>>>> f514872 (final and updated)
    msg.message_text,
    msg.message_length,
    msg.view_count,
    msg.forward_count,
    msg.has_image
from msg
<<<<<<< HEAD
join ch using (channel_name);
=======
join ch on msg.channel_name = ch.channel_name
join dt on msg.full_date = dt.full_date
>>>>>>> f514872 (final and updated)
