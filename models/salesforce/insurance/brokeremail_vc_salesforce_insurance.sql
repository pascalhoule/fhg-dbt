{{  config(
    alias='brokeremail_vc', 
    database='salesforce', 
    schema='insurance')  }}

SELECT *
FROM {{ ref('brokeremail_vc_clean_insurance') }}
