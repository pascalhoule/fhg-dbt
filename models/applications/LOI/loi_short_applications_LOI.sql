 {{  config(alias='loi_short', database='applications', schema='LOI', materialized = "view")  }} 


SELECT DISTINCT
fa_ods.fundaccount_number ACCOUNT_NUMBER
,r_ods.legal_name CLIENTNAME
,fa_cur.accounttype ACCOUNT_TYPE
,fa_cur.account_status
,fp_cur.fundid
,s_cur.name SPONSORNAME
,s_ods.address STREET
,s_ods.city
,s_ods.state_code PROVINCE
,s_ods.zip_code POSTCODE
,s_ods.market_email_address EMAIL
,s_ods.market_fax FAX
FROM
{{ ref ('fundaccount_applications_investment')  }} fa_ods
LEFT JOIN {{ ref ('registration_applications_investment')  }} r_ods on r_ods.code = fa_ods.registration_code
LEFT JOIN {{ ref ('fundaccount_vc_applications_investment')  }} fa_cur on fa_cur.fundaccountcode = fa_ods.code
LEFT JOIN {{ ref ('fundproducts_vc_applications_investment')  }} fp_cur on fp_cur.fundproductcode = fa_cur.fundproduct_code
LEFT JOIN {{ ref ('sponsors_vc_applications_investment')  }} s_cur on s_cur.sponsorid = fp_cur.sponsorid
LEFT JOIN {{ ref ('sponsor_applications_investment')  }} s_ods on s_ods.code = s_cur.code