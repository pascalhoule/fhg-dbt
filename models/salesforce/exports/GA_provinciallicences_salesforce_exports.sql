{{ config(
    alias='GA_provinciallicences', 
    database='salesforce', 
    schema='exports',
   materialized="view",
   grants = {'ownership': ['FH_READER']}) }}

With Temp As (
    Select Pl.*
    From
        {{ ref('provinciallicences_salesforce_exports') }} As Pl
    Where Exists (
        Select 1
        From {{ ref('broker_salesforce_exports') }} As Bt
        Where Bt.Agentcode = Pl.Agentcode
    )
)

Select * From Temp
