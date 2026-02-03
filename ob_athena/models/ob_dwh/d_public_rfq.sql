{{ config(
    materialized='table',
    file_format='parquet',
    external_location='s3://obranding-datalake-silver-zone-dev/db/ob_dwh/d_public_rfq/'
) }}

select 
	a.id public_rfq_id
	,cast(a.created_at.member0 as timestamp) as created_at
	, a.creator_id
	, a.from_brand_id from_brand_id
	, a.name
	, a.description
	,cast(a.expired_date.member0 as timestamp) as expired_date
	, a.status
	, a.approval_status 
	, a.approval_note 
	, a.request_item_count 
	, a.request_unit 
	, a.request_budget 
	, a.requestCurrency request_currency
	, a.metadata 
	, a.has_order 
	, b.b2b_category_id 
	, c.name_en category_name_en
	, c.name_vi category_name_vi
	, c.description category_description 
from {{source('open_live_market','b2b_public_rfq')}}  a 
left join {{source('open_live_market','b2b_public_rfq_category')}} b on a.id = b.b2b_public_rfq_id
left join {{source('open_live_market','b2b_category')}}  c on b.b2b_category_id = c.id