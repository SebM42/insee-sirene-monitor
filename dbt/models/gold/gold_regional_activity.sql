{{
    config(
        materialized='table',
        schema='sirene'
    )
}}

select
    month,
    department,
    ape_code,
    nb_active,
    nb_creations,
    nb_closures,
    nb_reopenings,
    nb_active - lag(nb_active) over (
        partition by department, ape_code
        order by month
    ) as net_growth,
    case
        when nb_active > 0
        then round((nb_creations + nb_reopenings - nb_closures) * 100.0 / nb_active, 2)
        else null
    end as dynamism_score
from {{ ref('gold_sector_trends') }}
order by 1, 2, 3