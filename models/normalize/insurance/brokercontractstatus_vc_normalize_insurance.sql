{{  config(alias='brokercontractstatus_vc', database='normalize', schema='insurance')  }} 


SELECT
    CODE,
    ENGLISHDESCRIPTION,
    FRENCHDESCRIPTION,
    INDEXSEQUENCE 
FROM
  {{ ref ('brokercontractstatus_vc_clean_insurance') }}