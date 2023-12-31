 {{  config(alias='ins_broker', database='integrate', schema='insurance')  }} 


with cte_tags as (
    select
        agentcode, listagg(tagname, ', ') tags
    from
        fh_prod.wealthserv_ins_curated_secure.brokertags_vc
    group by agentcode
)


-- insurance broker
-- create or replace temp table ins_broker as
select 
userdefined2,
case when try_cast(userdefined2 as integer) > 9999999 then userdefined2 else null end as ud2,
b.brokerid = ba.userdefined2 or ((try_cast(ba.userdefined2 as integer) > 9999999) is null or not try_cast(ba.userdefined2 as integer) > 9999999) as is_prim_rep,
b.brokerid, 
b.agentcode, 
b.firstname, b.
lastname, 
b.dateofbirth as DOB, 
b.sin, b.agentstatus, 
b.agenttype, 
b.companyname,
h.nodename as hierarchy,
concat( adr.address, ' ', adr.city, ' ',  adr.province, ' ',  adr.postal_code, ' ', adr.country ) as bus_address,
phone.number as bus_phone,
email.emailaddress as bus_email,
t.tags
from fh_prod.wealthserv_ins_curated_secure.broker_vc b
inner join {{ ref('hierarchy_vc_normalize_insurance') }} h on h.hierarchycode = b.agacode
left join  {{ ref('brokeradvanced_vc_normalize_insurance') }} ba on ba.agentcode = b.agentcode
left join  {{ ref('brokeraddress_vc_normalize_insurance') }} adr on adr.agentcode = b.agentcode and adr.type = 'business'
left join  {{ ref('brokerphone_vc_normalize_insurance') }} phone on phone.agentcode = b.agentcode and phone.type = 'Business'
left join  {{ ref('brokeremail_vc_normalize_insurance') }} email on email.agentcode = b.agentcode and email.type = 'business'
left join cte_tags t on t.agentcode = b.agentcode
where UserDefined2 LIKE '3268%' or UserDefined2 LIKE '3162%'
