{{ config(
    materialized='table',
    file_format='parquet',
    external_location='s3://obranding-datalake-silver-zone-dev/db/ob_dwh/d_b2b_supplier/'
) }}

select 
	a.brand_id supplier_id 
	, b.created_at supplier_created_at 
	, b.creator_id supplier_creator_id
	, a.id supplier_profile_id 
	, a.created_at supplier_profile_created_at
	, a.creator_id supplier_profile_creator_id
	, a.b2b_profile
	, a.is_verified 
	, a.is_hot 
	, a.keywords
	, a.status 
	, a.legal_info
	, a.home_content 
	, a.approval_status 
	, a.approval_note 
	, a.is_regulated_business
	, round(date_diff('year', 
		date(from_iso8601_timestamp(json_extract_scalar(a.legal_info, '$.issuedDate'))), 
		current_date), 0) as brand_time_from_open
from {{source('open_live_market','b2b_brand_profile')}} a
left join {{source('open_live_market','loyalty_brand')}} b on a.brand_id = b.id