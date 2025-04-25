{{  config(alias='ins_brokeradvanced_vc', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM
{{ ref('brokeradvanced_vc_clean_insurance') }}