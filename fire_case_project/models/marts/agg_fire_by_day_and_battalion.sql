{{ config(materialized='table') }}

SELECT
    incident_date,
    battalion,
    COUNT(*) AS total_incidents,
    SUM(estimated_property_loss) AS total_property_loss
FROM {{ ref('stg_fire_incidents') }}
GROUP BY incident_date, battalion
ORDER BY incident_date, battalion