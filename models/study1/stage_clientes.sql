WITH clientes AS (
  SELECT * FROM {{ ref("clientes") }}
)

SELECT *, 
       age > 25 AS maiority
FROM schema.Clientes

