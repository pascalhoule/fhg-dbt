{{ config( alias='__base_adj', database='agt_comm', schema='contest', materialized = "view" ) }} 

SELECT
    *
FROM
    {{ source('contest', 'adjustments') }}
WHERE
    MTH = MONTH(
        (
            select
                max(SEG_FUNDS_ENDDATE)
            from
                {{ source('contest', 'date_ranges') }} 
        )
    )
    and YR = YEAR(
        (
            select
                max(SEG_FUNDS_ENDDATE)
            from
                {{ source('contest', 'date_ranges') }} 
        )
    )