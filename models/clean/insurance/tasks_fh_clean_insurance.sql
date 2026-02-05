{{ config(alias='tasks_fh', database='clean', schema='insurance') }}

SELECT
    *

FROM {{ source('task_insurance','task') }}