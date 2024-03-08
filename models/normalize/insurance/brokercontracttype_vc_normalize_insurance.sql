{{  config(alias='brokercontracttype_vc', database='normalize', schema='insurance')  }} 


SELECT
    CODE,
    ENGLISHDESCRIPTION,
    FRENCHDESCRIPTION,
    INDEXSEQUENCE 
FROM
  {{ ref ('brokercontracttype_vc_clean_insurance') }}