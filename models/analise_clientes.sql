WITH stage_clientes AS (
    SELECT * FROM {{ ref("stage_clientes") }}
)

SELECT 
    maiority, 
    count(maiority) as percentyy
FROM stage_clientes
GROUP BY maiority
