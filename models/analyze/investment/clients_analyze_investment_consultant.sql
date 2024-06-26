 {{  config(alias='clients_consultant', database='analyze', schema='investment')  }} 


select 
cl.investment_client_code
,cl.investment_client_first_name
,cl.investment_client_last_name
,cl.investment_client_clientid
,cl.investment_client_city
,cl.investment_client_clientsince
,cl.investment_client_country_code
,cl.investment_client_dob
, case 
    when datediff('year', cl.investment_client_dob, CURRENT_DATE() ) <  25 then 'Under 25' 
    when datediff("year", cl.investment_client_dob, CURRENT_DATE() ) >= 25 and datediff("year", cl.investment_client_dob, CURRENT_DATE() ) < 42 then '25 - 41' 
    when datediff("year", cl.investment_client_dob, CURRENT_DATE() ) >= 42 and datediff("year", cl.investment_client_dob, CURRENT_DATE() ) < 58 then '42 - 57' 
    when datediff("year", cl.investment_client_dob, CURRENT_DATE() ) >= 58 and datediff("year", cl.investment_client_dob, CURRENT_DATE() ) < 77 then '58 - 76' 
	when datediff("year", cl.investment_client_dob, CURRENT_DATE() ) >= 77 then '77+'
    else 'unknown'
    end as investment_client_age_range
,cl.investment_client_rep_code
,cl.investment_client_ssn
,cl.investment_client_state_code
,ifnull(status.description, 'Unknown') as investment_client_status
,ifnull(status.description2, 'Unknown') as investment_client_status_fr
,cl.investment_client_street
,cl.investment_client_zipcode
,cl.investment_client_taxid
,ifnull(sex.description, 'Unknown') as investment_client_sex
,ifnull(sex.description2, 'Unknown') as investment_client_sex_fr
,state.investment_state_name

,cl.investment_client_email
,cl.investment_client_email2
,case when cl.investment_client_cell_phone = '(   )   -' then null else cl.investment_client_cell_phone end as investment_client_cell_phone
,lang.description as investment_client_client_language

from {{ ref ('clients_integrate_investment_consultant')  }} cl
left join {{ ref ('state_integrate_investment')  }} state on cl.investment_client_state_code = state.investment_state_code
left join {{ ref ('constants_integrate_investment') }} sex on sex.value = cl.investment_client_sex_id and sex.type = 'Sex'
left join {{ ref ('constants_integrate_investment') }} status on status.value = cl.investment_client_status and status.type = 'ClientStatus'
left join {{ ref ('constants_integrate_investment') }} lang on lang.value = cl.investment_client_languagepreference and lang.type = 'Language'
