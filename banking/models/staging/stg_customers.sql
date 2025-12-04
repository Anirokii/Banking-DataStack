{{ config(materialized='view') }}

with ranked as (
    select
        v.id as customer_id,
        v.first_name,
        v.last_name,
        v.email,
        v.created_at,
        current_timestamp as load_timestamp,
        row_number() over (
            partition by v.id
            order by v.created_at desc
        ) as rn
    from {{ source('raw','customers') }} as v
)

select *
from ranked
where rn = 1
