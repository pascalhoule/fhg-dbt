 {{ config(alias='clients', database='integrate', schema='insurance') }}

select 
c.insurance_client_code
,c.insurance_client_id
,c.insurance_client_first_name
,c.insurance_client_last_name
,c.insurance_client_jointaccountstatus
,c.insurance_client_main_address_attn_line
,c.insurance_client_main_address_attn_prefix
,c.insurance_client_cell_phone
,c.insurance_client_home_phone
,c.insurance_client_work_phone
,c.insurance_client_extention_phone
,c.insurance_client_other_phone
,c.insurance_client_dob
,c.insurance_client_gender_id
,c.insurance_client_smoker_status
,c.insurance_client_email
,c.insurance_client_email2
,c.insurance_client_country_id
,c.insurance_client_main_address_city
,c.insurance_client_main_address_street
,c.insurance_client_main_address_street2
,c.insurance_client_main_address_street3
,c.insurance_client_main_address_zipcode
,c.insurance_client_main_address_state_id
,c.insurance_client_client_language
,c.insurance_client_agentcode
,c._infx_loaded_ts_utc 
,c._infx_active_from_ts_utc 
,c._infx_active_to_ts_utc 
,c._infx_is_active


from {{ ref ('clients_normalize_insurance') }} c
