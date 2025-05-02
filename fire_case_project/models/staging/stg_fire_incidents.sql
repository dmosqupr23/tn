{{ config(
    materialized='incremental',
    unique_key='id'
) }}

SELECT *
FROM {{ source('public', 'fire_incidents_desnormalized') }}
{% if is_incremental() %}
WHERE incident_date > (SELECT MAX(incident_date) FROM {{ this }})
{% endif %}