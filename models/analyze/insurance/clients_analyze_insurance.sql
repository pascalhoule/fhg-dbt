 {{ config(alias='clients', database='analyze', schema='insurance') }}

select 
c.insurance_client_code
,c.insurance_client_id
,c.insurance_client_first_name
,c.insurance_client_last_name
,c.insurance_client_jointaccountstatus
,c.insurance_client_country_id
,c.insurance_client_main_address_city
,c.insurance_client_main_address_street
,c.insurance_client_main_address_street2
,c.insurance_client_main_address_street3
,c.insurance_client_main_address_zipcode
,c.insurance_client_main_address_attn_line
,c.insurance_client_main_address_attn_prefix
,s.state_name as insurance_client_main_address_state_name
,c.insurance_client_cell_phone
,c.insurance_client_home_phone
,c.insurance_client_work_phone
,c.insurance_client_extention_phone
,c.insurance_client_other_phone
,c.insurance_client_dob
, case 
    when datediff('year', c.insurance_client_dob, CURRENT_DATE() ) <  25 then 'Under 25' 
    when datediff("year", c.insurance_client_dob, CURRENT_DATE() ) >= 25 and datediff("year", c.insurance_client_dob, CURRENT_DATE() ) < 42 then '25 - 41' 
    when datediff("year", c.insurance_client_dob, CURRENT_DATE() ) >= 42 and datediff("year", c.insurance_client_dob, CURRENT_DATE() ) < 58 then '42 - 57' 
    when datediff("year", c.insurance_client_dob, CURRENT_DATE() ) >= 58 and datediff("year", c.insurance_client_dob, CURRENT_DATE() ) < 77 then '58 - 76' 
    when datediff("year", c.insurance_client_dob, CURRENT_DATE() ) >= 77 then '77+'
    else 'unknown'
    end as insurance_client_age_range
,ifnull(cs_sex.description, 'Unknown') as insurance_client_gender
,c.insurance_client_smoker_status
,c.insurance_client_smoker_status_fr
,c.insurance_client_email
,c.insurance_client_email2
-- ,c.insurance_client_state_id
,cs_lang.description as insurance_client_client_language
,c.insurance_client_agentcode
,c._infx_loaded_ts_utc 
,c._infx_active_from_ts_utc 
,c._infx_active_to_ts_utc 
,c._infx_is_active


from {{ ref ('clients_integrate_insurance') }} c
left join {{ ref ('state_integrate_insurance') }} s on c.insurance_client_main_address_state_id = s.state_id
left join {{ ref ('constants_integrate_insurance') }} cs_sex on cs_sex.value = c.insurance_client_gender_id and cs_sex.type = 'Sex'
left join {{ ref ('constants_integrate_insurance') }} cs_lang on cs_lang.value = c.insurance_client_client_language and cs_lang.type = 'Preferred_Language'

