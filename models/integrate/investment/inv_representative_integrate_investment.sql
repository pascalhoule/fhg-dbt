 {{  config(alias='inv_representative', database='integrate', schema='investment')  }} 

select 
case when try_cast(userdefined2 as integer) > 9999999 then userdefined2 else null end as ud2,
rep.repid = ba.userdefined2 or ((try_cast(ba.userdefined2 as integer) > 9999999) is null or not try_cast(ba.userdefined2 as integer) > 9999999) as is_prim_rep,
phone.number as bus_phone,
email.emailaddress as bus_email,
concat( adr.address, ' ', adr.city, ' ',  adr.province, ' ',  adr.postalcode, ' ', adr.country ) as bus_address,
rep.representiativecode, rep.insagentcode, rep.repid, rep.first_name, rep.last_name, rep.DOB, rep.branch_code, rep.sin, rep.repstatus
from {{ ref('representatives_vc_normalize_investment') }} rep
left join {{ ref('brokeradvanced_vc_normalize_insurance') }} ba on ba.agentcode = rep.insagentcode
left join {{ ref('representativeaddress_vc_normalize_investment') }} adr on rep.code = adr.representativecode and adr.type = 'business'
left join {{ ref('brokeremail_vc_normalize_insurance') }} email on email.agentcode = rep.insagentcode and email.type = 'business'
left join {{ ref('brokerphone_vc_normalize_insurance') }} phone on phone.agentcode = rep.insagentcode and phone.type = 'Business'
where rep.repid  not in ('33381755', '33381456')
