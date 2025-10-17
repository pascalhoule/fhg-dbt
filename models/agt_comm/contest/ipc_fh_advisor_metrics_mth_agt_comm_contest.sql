{{ config( 
    alias='ipc_fh_advisor_metrics_mth', 
    database='agt_comm', 
    schema='contest', 
    materialized = "view" ) }}

SELECT * FROM
{{ source('ac_direct_current', 'ipc_fh_advisor_metrics_mth_new') }}