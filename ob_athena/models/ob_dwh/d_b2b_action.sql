{{ config(
    materialized='table',
    file_format='parquet',
    external_location='s3://obranding-datalake-silver-zone-dev/db/ob_dwh/d_b2b_action/'
) }}
{{ config(
    materialized='table',
    file_format='parquet',
    external_location='s3://obranding-datalake-silver-zone-dev/db/ob_dwh/d_b2b_action/'
) }}
select 
    CAST(f.buyer_id AS VARCHAR) as user_id,
    array_join(array_agg(CAST(p.id AS VARCHAR) ORDER BY f.created_at DESC), ',') as list_product_ids,
    array_join(array_agg(p.category_name_vi), ',') as list_categories,
    array_join(array_agg(CAST(s.supplier_id AS VARCHAR)), ',') as list_suppliers,
    MIN(p.product_min_price) as min_product_price,
    MAX(p.product_max_price) as max_product_price,
    MIN(f.b2b_min_volume) as min_volume,
    MAX(f.b2b_max_volume) as max_volume
from {{ref('fact_action')}} f
LEFT JOIN {{ref('d_product')}} p ON f.b2b_product_id = p.id
LEFT JOIN {{ref('d_b2b_supplier')}} s on f.seller_brand_id = s.supplier_id
where f.buyer_id is not null
group by 1