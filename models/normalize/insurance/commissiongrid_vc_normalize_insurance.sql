 {{  config(alias='commissiongrid_vc', database='normalize', schema='insurance')  }} 

 SELECT *
 FROM {{ ref('commissiongrid_vc_clean_insurance') }}
