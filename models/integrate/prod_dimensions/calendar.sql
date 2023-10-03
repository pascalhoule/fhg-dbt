{{			
    config (			
        materialized="table",			
        alias='calendar', 			
        database='integrate', 			
        schema='dimensions'			
    )			
}}			
			
WITH 			
CTE_DATE AS (			
    SELECT 			
    DATEADD(DAY, SEQ4(), '1900-01-01') AS MY_DATE			
    ,SEQ4() AS INDEX			
    FROM TABLE(GENERATOR(ROWCOUNT=>100000))			
)			
SELECT    			
    INDEX			
    ,MY_DATE::date AS DATE			
    ,TO_varchar(YEAR(MY_DATE),'9999') AS YEAR			
    ,MONTH(MY_DATE) AS MONTH			
    ,DAY(MY_DATE) AS DAY			
    ,QUARTER(MY_DATE) AS QUARTER			
    ,DAYOFWEEK(MY_DATE) AS DAY_OF_WEEK			
    ,WEEKOFYEAR(MY_DATE) AS WEEK_OF_YEAR			
    ,DAYOFYEAR(MY_DATE) AS DAY_OF_YEAR			
    ,MONTHNAME(MY_DATE) AS MONTH_NAME_EN_SHORT			
    ,CASE WHEN MONTH(MY_DATE) < 7 THEN 1 ELSE 2 END AS SEMESTER			
    ,m.MONTH_NAME_EN			
    ,m.MONTH_NAME_FR_SHORT			
    ,m.MONTH_NAME_FR			
FROM CTE_DATE c JOIN {{ source('dimensions', 'month_name')}} m on MONTHNAME(MY_DATE) = m.MONTH_NAME_EN_SHORT			
			
