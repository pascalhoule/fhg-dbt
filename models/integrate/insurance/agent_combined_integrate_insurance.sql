 {{  config(alias='agent_combined', database='integrate', schema='insurance')  }} 


select distinct
concat( ins.firstname, ' ', ins.lastname, ' ', coalesce(ins.ud2, ins.agentcode::string)  ) as person_uid,
ins.ud2, ins.is_prim_rep, ins.firstname, ins.lastname, ins.dob, ins.sin, ins.bus_address, ins.bus_email, ins.bus_phone, 
ins.agentcode as ins_agentcode
from {{ ref ('ins_broker_integrate_insurance')  }}  ins
inner join {{ ref ('inv_representative_integrate_investment')  }} inv on ins.agentcode = inv.insagentcode 

