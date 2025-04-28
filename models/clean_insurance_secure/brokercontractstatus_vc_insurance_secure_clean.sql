{{  config(alias='ins_brokercontractstatus_vc', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM
{{ ref('brokercontractstatus_vc_clean_insurance') }}