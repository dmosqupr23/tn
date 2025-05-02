{{ config(materialized='table') }}

SELECT
    incident_date,
    COUNT(*) AS total_incidents,
    COUNT(DISTINCT id) AS unique_incidents,
    SUM(estimated_property_loss) AS total_property_loss,
    SUM(estimated_contents_loss) AS total_contents_loss
FROM {{ ref('stg_fire_incidents') }}
GROUP BY incident_date
ORDER BY incident_date
