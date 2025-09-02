{{  config(alias='dpr_cos_comm_agt_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('dpr_cos_comm_agt_fh_for_share_clean') }}