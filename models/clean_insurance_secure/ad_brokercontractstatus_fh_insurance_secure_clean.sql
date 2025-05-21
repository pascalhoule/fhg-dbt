{{  config(alias='ad_brokercontractstatus_fh', 
database='clean', 
schema='insurance_secure')  }} 

SELECT * FROM {{ ref('ad_brokercontractstatus_fh_for_share_clean') }}