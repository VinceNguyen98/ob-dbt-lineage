{{ config(
    materialized='table',
    file_format='parquet',
    external_location='s3://obranding-datalake-silver-zone-dev/db/ob_dwh/d_product/'
) }}

select 
	a.id
	, a.category_id	
	, a.brand_id supplier_id   
	, a.name 
	, a.description 
	, a.keywords 
	, a.info
	, a.approval_status 
	, a.brand_sku 
	, a.status
	, a.is_featured  
	, a.is_top_search 
	, json_extract_scalar(a.info, '$.basicInfo.variants.ORIGIN.value') AS product_origin
	, array_join(array_agg(c.name), ', ') AS product_certificate_name 
	, array_join(array_agg(c.status), ', ') AS product_certificate_status
	, d.name_en category_name_en
	, d.name_vi category_name_vi
	, d.description category_description
	, min(f.min_volume) product_min_volume 
	, max(f.max_volume) product_max_volume 
	, min(price) product_min_price
	, max(price) product_max_price 
from {{ source('open_live_market', 'b2b_product') }} a   
left join {{ source('open_live_market', 'b2b_product_cert') }} b on a.id = b.b2b_product_id  
left join {{ source('open_live_market', 'b2b_certificate') }} c on b.b2b_certificate_id  = c.id
left join {{ source('open_live_market', 'b2b_category') }} d on a.category_id = d.id 
left join {{ source('open_live_market', 'b2b_product_sku') }} e on a.id = e.b2b_product_id  
left join {{ source('open_live_market', 'b2b_product_sku_price_range') }} f on e.id = f.b2b_product_sku_id 
group by a.id, a.category_id, a.brand_id, a.name, a.description, a.keywords, a.info, a.approval_status, a.brand_sku, a.status, a.is_featured, a.is_top_search, d.name_en, d.name_vi, d.description