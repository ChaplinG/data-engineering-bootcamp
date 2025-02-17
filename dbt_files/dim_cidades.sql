{{ config(materialized='table') }}
SELECT
    id_cidades AS cidade_id,
    nome_cidade,
    id_estados AS estado_id,
    data_inclusao,
    data_atualizacao
FROM {{ ref('stg_cidades') }} -- ref uses an object of dbt model, not on the original db
