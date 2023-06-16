 {{ config(alias='clients', database='normalize', schema='insurance') }}

select 
 c.code as insurance_client_code
,c.clientid as insurance_client_id
,c.first_name as insurance_client_first_name
,c.last_name as insurance_client_last_name
,c.jointaccountstatus as insurance_client_jointaccountstatus
,c.address_attn_line as insurance_client_main_address_attn_line
,c.address_attn_prefix  as insurance_client_main_address_attn_prefix
,c.cell_phone as insurance_client_cell_phone
,c.home_phone as insurance_client_home_phone
,c.work_phone as insurance_client_work_phone
,c.extention_phone as insurance_client_extention_phone
,c.other_phone as insurance_client_other_phone
,c.dob as insurance_client_dob
,c.sex as insurance_client_gender_id
,c.smoker as insurance_client_smoker_status
,c.email as insurance_client_email
,c.email2 as insurance_client_email2
,c.country_code as insurance_client_country_id
,c.city as insurance_client_main_address_city
,c.street as insurance_client_main_address_street
,c.street2 as insurance_client_main_address_street2
,c.street3 as insurance_client_main_address_street3
,c.zipcode as insurance_client_main_address_zipcode
,c.state_code as insurance_client_main_address_state_id
,c.client_language as insurance_client_client_language
,c.agentcode as insurance_client_agentcode
,c.status as insurance_client_status_code
,_infx_loaded_ts_utc 
,_infx_active_from_ts_utc 
,_infx_active_to_ts_utc 
,_infx_is_active

from {{ ref ('clients_clean_insurance') }} c
