-- models/marts/stg_dim_product.sql
{{ config(
    materialized='table'
) }}

WITH source AS (
    SELECT
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
    FROM {{ source('raw', 'products_info') }} s
),

preference_order AS (
    SELECT
        p.store_code,
        p.order
    FROM {{ source('raw', 'store_code_preference')}} p
),

raw_join AS (
    SELECT
        s.*,
        p.order
    FROM source s
    LEFT JOIN preference_order p ON s.store_code = p.store_code
)

SELECT distinct
  r.product_id AS product_key,
  r.name AS product_name,
  r.sku,
  r.gender,
  r.collection AS collection_name,
  r.category AS category_name,
  r.product_type,
  r.gold_weight,
  r.platinum_palladium_info_in_alloy
FROM raw_join r
LEFT JOIN (
  SELECT
    rj.product_id,
    min(rj.order) AS min_order
  FROM raw_join rj
  GROUP BY 1
) AS j ON r.product_id = j.product_id
WHERE r.order = j.min_order