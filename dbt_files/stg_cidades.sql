{{ config(materialized='view') }}
--jija2 template; it defines the type of materialization we'll use
WITH source AS ( -- source gets data from a table outside of dbt, the original table
    SELECT
        id_cidades,
        INITCAP(cidade) AS nome_cidade, 
        id_estados,
        data_inclusao,
        COALESCE(data_atualizacao, data_inclusao) AS data_atualizacao -- returns the first NULL value
    FROM {{ source('sources', 'cidades') }}
) -- the result of this SELECT will be storaged in "source", which is used in the code below

SELECT
    id_cidades,
    nome_cidade,
    id_estados,
    data_inclusao,
    data_atualizacao
FROM source