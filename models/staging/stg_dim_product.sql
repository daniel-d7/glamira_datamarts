-- models/marts/stg_dim_product.sql
{{ config(
    materialized='table'
) }}

with source as (
    select
        s.product_id,
        s.name,
        s.sku,
        s.gender,
        s.category_name,
        s.collection,
        s.store_code,
        s.category,
        s.product_type,
        s.gold_weight,
        s.platinum_palladium_info_in_alloy
    from {{ source('raw', 'products_info') }} s
),

preference_order as (
    select
        p.store_code,
        p.order
    from {{ source('raw', 'store_code_preference')}} p
),

raw_join as (
    select
        s.*,
        p.order
    from source s
    left join preference_order p on s.store_code = p.store_code
)

select distinct
  r.product_id as product_key,
  r.name as product_name,
  r.sku,
  r.gender,
  r.collection as collection_name,
  r.category as category_name,
  r.product_type,
  r.gold_weight,
  r.platinum_palladium_info_in_alloy
from raw_join r
left join (
  select
    rj.product_id,
    min(rj.order) as min_order
  from raw_join rj
  group by 1
) as j on r.product_id = j.product_id
where r.order = j.min_order