{{ config(materialized='view') }}

with ranked as (
    select
        v.id as account_id,
        v.customer_id,
        v.account_type,
        v.balance,
        v.currency,
        v.created_at,
        current_timestamp as load_timestamp,
        row_number() over (
            partition by v.id
            order by v.created_at desc
        ) as rn
    from {{ source('raw','accounts') }} as v
)

select *
from ranked
where rn = 1
