{{  config(alias='ins_brokeraddress_vc', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM
{{ ref('brokeraddress_vc_clean_insurance') }}