{{ config(
    materialized='table',
    file_format='parquet',
    external_location='s3://obranding-datalake-silver-zone-dev/db/ob_dwh/d_product_sku/'
) }}

select 
	a.id b2b_product_sku_id 
	, a.b2b_product_id  
	, a.brand_sku 
	, a.sku 
	, a.commercial_variant_attributes  
	, a.status 
	, a.retail_price 
	, a.stock_quantity 
	, a.name product_sku_name
	, b.min_volume  
	, b.max_volume 
	, b.price 
from {{source('open_live_market','b2b_product_sku')}} a 
left join {{source('open_live_market','b2b_product_sku_price_range')}} b 
on a.id = b.b2b_product_sku_id 