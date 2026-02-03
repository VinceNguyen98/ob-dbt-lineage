{{ config(
    materialized='table',
    file_format='parquet',
    external_location='s3://obranding-datalake-silver-zone-dev/db/ob_dwh/fact_action/'
) }}


with create_prod_sku as 
( 
select 
	'CREATE_PRODUCT' interact_type
	, 'seller' interact_side
	, a.creator_id seller_id 
	, b.brand_id seller_brand_id
	, NULL buyer_id
	, NULL buyer_brand_id 
	, b.id b2b_product_id
	, b.category_id b2b_category_id 
	, a.id b2b_product_sku_id
	,cast(a.created_at.member0 as timestamp) as created_at 
	, NULL rfq_code -- join code vào bảng b2b_direct_rfq để lấy thông tin  
	, NULL rfq_item_count 
	, NULL rfq_amount   
	, NULL rfq_unit      
	, NULL quote_item_count  
	, NULL quote_offered_price    
from {{ source('open_live_market', 'b2b_product_sku') }} a 
left join {{ source('open_live_market', 'b2b_product') }} b on a.b2b_product_id = b.id 
)
,
create_direct_rfq as ( 
select 
	'CREATE_DIRECT_RFQ' interact_type
	, 'buyer' interact_side
	, NULL seller_id
	, b.brand_id seller_brand_id 
	, a.creator_id buyer_id
	, a.from_brand_id buyer_brand_id
	, a.b2b_product_id b2b_product_id
	, b.category_id b2b_category_id
	, c.b2b_product_sku_id b2b_product_sku_id
	,cast(a.created_at.member0 as timestamp) as created_at
	, a.code rfq_code
	, c.item_count rfq_item_count   
	, c.amount rfq_amount
	, c.unit rfq_unit 
	, NULL quote_item_count  
	, NULL quote_offered_price    
from {{ source('open_live_market', 'b2b_direct_rfq') }} a
left join {{ source('open_live_market', 'b2b_product') }} b on a.b2b_product_id = b.id 
left join {{ source('open_live_market', 'b2b_direct_rfq_detail') }} c on a.id = c.b2b_direct_rfq_id 
)
, 
create_direct_quote as ( 
select 
	'CREATE_DIRECT_QUOTE' interact_type
	, 'seller' interact_side
	, a.creator_id seller_id
	, a.brand_id seller_brand_id 
	, b.creator_id buyer_id 
	, b.from_brand_id buyer_brand_id
	, b.b2b_product_id 
	, c.category_id  b2b_category_id
	, d.b2b_product_sku_id b2b_product_sku_id 
	,cast(a.created_at.member0 as timestamp) as created_at
	, b.code rfq_code 
	, d.item_count rfq_item_count   
	, d.amount rfq_amount
	, d.unit rfq_unit 
	, a1.item_count quote_item_count  
	, a1.offered_price quote_offered_price  
from {{ source('open_live_market', 'b2b_direct_quote') }} a 
left join {{ source('open_live_market', 'b2b_direct_quote_detail') }} a1 on a.id = a1.id
left join {{ source('open_live_market', 'b2b_direct_rfq') }} b on a.b2b_direct_rfq_id = b.id
left join {{ source('open_live_market', 'b2b_product') }} c on b.b2b_product_id = c.id 
left join {{ source('open_live_market', 'b2b_direct_rfq_detail') }} d on b.id = d.b2b_direct_rfq_id 
)
,
create_public_rfq as ( 
select 
	'CREATE_PUBLIC_RFQ' interact_type
	, 'buyer' interact_side
	, NULL seller_id 
	, NULL seller_brand_id
	, a.creator_id buyer_id 
	, a.from_brand_id buyer_brand_id 
	, NULL b2b_product_id 
	, b.b2b_category_id b2b_category_id 
	, NULL b2b_product_sku_id 
	,cast(a.created_at.member0 as timestamp) as created_at
	, a.code rfq_code  
	, a.request_item_count rfq_item_count   
	, a.request_budget rfq_amount
	, a.request_unit rfq_unit 
	, NULL quote_item_count  
	, NULL quote_offered_price 
from {{ source('open_live_market', 'b2b_public_rfq') }} a 
left join {{ source('open_live_market', 'b2b_public_rfq_category') }} b on a.id = b.b2b_public_rfq_id 
)
,
create_quote_to_public_rfq as (
select 
	'CREATE_QUOTE_TO_PUBLIC_RFQ' interact_type 
	, 'seller' interact_side
	, a.creator_id seller_id 
	, a.brand_id seller_brand_id
	, b.creator_id buyer_id 
	, b.from_brand_id buyer_brand_id
	, c.b2b_product_id 
	, b1.b2b_category_id b2b_category_id
	, c.id b2b_product_sku_id 
	,cast(a.created_at.member0 as timestamp) as created_at
	, b.code rfq_code   
	, b.request_item_count rfq_item_count   
	, b.request_budget rfq_amount
	, b.request_unit rfq_unit 
	, a.offered_item_count  quote_item_count  
	, a.offered_price quote_offered_price 
from {{ source('open_live_market', 'b2b_quote_to_public_rfq') }} a 
left join {{ source('open_live_market', 'b2b_public_rfq') }} b on a.b2b_public_rfq_id = b.id 
left join {{ source('open_live_market', 'b2b_public_rfq_category') }} b1 on b.id = b1.b2b_public_rfq_id 
left join {{ source('open_live_market', 'b2b_product_sku') }} c on a.b2b_product_sku_id = c.id
)
,
create_order as ( 
select 
	'CREATE_ORDER' interact_type 
	, 'buyer' interact_side
	, NULL seller_id
	, a.supplier_id seller_brand_id 
	, a.creator_id buyer_id
	, a.brand_id buyer_brand_id
	, b3.id b2b_product_id
	, b3.category_id b2b_category_id 
	, b2.id b2b_product_sku_id
	,cast(a.created_at.member0 as timestamp) as created_at
	, NULL rfq_code   
	, NULL rfq_item_count   
	, NULL rfq_amount
	, NULL rfq_unit 
	, NULL quote_item_count  
	, NULL quote_offered_price 
from {{ source('open_live_market', 'b2b_order') }} a 
left join {{ source('open_live_market', 'b2b_order_detail') }} b on a.id = b.b2b_order_id
left join {{ source('open_live_market', 'b2b_product_sku') }} b2  on b.b2b_product_sku_id  = b2.id 
left join {{ source('open_live_market', 'b2b_product') }} b3 on b3.id = b2.b2b_product_id 
)
,
consol as (
select * from create_prod_sku 
union all select * from create_direct_rfq 
union all select * from create_direct_quote
union all select * from create_public_rfq
union all select * from create_quote_to_public_rfq
union all select * from create_order
)
,

full_summary as 
(
select 
	a.*
	,b.b2b_certificate_id b2b_certificate_id
	,c.name b2b_certificate_name
	,json_extract_scalar(a1.info, '$.basicInfo.variants.ORIGIN.value') AS b2b_product_origin
	,d.min_volume b2b_min_volume 
	,d.max_volume b2b_max_volume 
	,d.price b2b_price
	, round(date_diff('year', 
		date(from_iso8601_timestamp(json_extract_scalar(e.legal_info, '$.issuedDate'))), 
		current_date), 0) as brand_time_from_open
from consol a 
left join {{ source('open_live_market', 'b2b_product') }} a1 on a.b2b_product_id = a1.id
left join {{ source('open_live_market', 'b2b_product_cert') }} b on a.b2b_product_id  = b.b2b_product_id 
left join {{ source('open_live_market', 'b2b_certificate') }} c on b.b2b_certificate_id = c.id
left join {{ source('open_live_market', 'b2b_product_sku_price_range') }} d on a.b2b_product_sku_id = d.b2b_product_sku_id 
left join {{ source('open_live_market', 'b2b_brand_profile') }} e on e.brand_id = a.seller_brand_id
)
select count (1) from full_summary --3788





