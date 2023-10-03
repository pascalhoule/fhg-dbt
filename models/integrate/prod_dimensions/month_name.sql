{{
    config (
        materialized="table",
        alias='month_name', 
        database='integrate', 
        schema='dimensions'
    )
}}

WITH source_data AS (
    SELECT
        'Jan' AS MONTH_NAME_EN_SHORT,
        'January' AS MONTH_NAME_EN,
        'janv.' AS MONTH_NAME_FR_SHORT,
        'janvier' AS MONTH_NAME_FR
    UNION ALL
    SELECT
        'Feb', 'February', 'févr.', 'février'
    UNION ALL
    SELECT
        'Mar', 'March', 'mars', 'mars'
    UNION ALL
    SELECT
        'Apr', 'April', 'avr.', 'avril'
    UNION ALL
    SELECT
         'May', 'May', 'mai', 'mai'
    UNION ALL
    SELECT
         'Jun', 'June', 'juin', 'juin'
    UNION ALL
    SELECT
         'Jul', 'July', 'juil.', 'juillet'
    UNION ALL
    SELECT
         'Aug', 'August', 'août', 'août'
    UNION ALL
    SELECT
         'Sep', 'September', 'sept.','septembre'
    UNION ALL
    SELECT     
         'Oct', 'October', 'oct.', 'octobre'
    UNION ALL
    SELECT
         'Nov', 'November', 'nov.', 'novembre'
    UNION ALL
    SELECT
         'Dec', 'December', 'déc.', 'décembre'
)

SELECT *
FROM source_data