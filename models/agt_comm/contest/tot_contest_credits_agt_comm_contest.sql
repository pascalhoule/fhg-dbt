{{ config( 
    alias='tot_contest_credits', 
    database='agt_comm', 
    schema='contest', 
    materialized = "view" ) }}

SELECT * FROM
{{ source('contest', 'total_contest_credits') }}  c 
join {{ source('contest', 'date_ranges') }}  d
WHERE c.MTH = d.QUADRUS_MTH and c.yr = d.QUADRUS_YR and d.incl_in_rpt = true    