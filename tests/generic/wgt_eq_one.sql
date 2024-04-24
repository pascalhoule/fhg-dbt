{% test wgt_eq_one(model, column_name, col_measure_to_test) %}

{{ config(severity = 'warn') }} --This sets the default severity if it is not set in the .yml file

SELECT
    {{ column_name }},
    ROUND(SUM( {{ col_measure_to_test }} ), 3) AS TOT_WGT
FROM
    {{ model }}
GROUP BY
    1
HAVING
    TOT_WGT <> 1

{% endtest %}