{{ config(materialized='view') }}

select
    v.id as transaction_id,
    v.account_id,
    v.amount,
    v.txn_type as transaction_type,
    v.related_account_id,
    v.status,
    v.created_at,
    current_timestamp as load_timestamp
from {{ source('raw','transactions') }} as v
