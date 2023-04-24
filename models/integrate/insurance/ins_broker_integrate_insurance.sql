 {{  config(alias='ins_broker', database='integrate', schema='insurance')  }} 


with cte_tags as (
    select
        agentcode, listagg(tagname, ', ') tags
    from
        fh_prod.wealthserv_ins_curated_secure.brokertags_vl
    group by agentcode
)


-- insurance broker
-- create or replace temp table ins_broker as
select 
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
from fh_prod.wealthserv_ins_curated_secure.broker_vl b
inner join fh_prod.wealthserv_ins_curated_secure.HIERARCHY_VL h on h.hierarchycode = b.agacode
left join fh_prod.wealthserv_ins_curated_secure.brokeradvanced_vl ba on ba.agentcode = b.agentcode
left join fh_prod.wealthserv_ins_curated_secure.brokeraddress_vl adr on adr.agentcode = b.agentcode and adr.type = 'business'
left join fh_prod.wealthserv_ins_curated_secure.brokerphone_vl phone on phone.agentcode = b.agentcode and phone.type = 'Business'
left join fh_prod.wealthserv_ins_curated_secure.brokeremail_vl email on email.agentcode = b.agentcode and email.type = 'business'
left join cte_tags t on t.agentcode = b.agentcode
