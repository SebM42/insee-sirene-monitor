{{
    config(
        materialized='table',
        schema='sirene'
    )
}}

with spine as (
    {{
        dbt_utils.date_spine(
            datepart="month",
            start_date="cast('2000-01-01' as date)",
            end_date="current_date()"
        )
    }}
),

months as (
    select date_trunc('month', date_month) as month_start
    from spine
),

dim as (
    select distinct
        date_trunc('month', m.month_start) as month,
        s.activitePrincipaleEtablissement as ape_code,
        left(s.codeCommuneEtablissement, 2) as department
    from months m
    cross join (
        select distinct
            activitePrincipaleEtablissement,
            codeCommuneEtablissement
        from {{ source('sirene', 'silver') }}
    ) s
),

active as (
    select
        date_trunc('month', m.month_start) as month,
        s.activitePrincipaleEtablissement as ape_code,
        left(s.codeCommuneEtablissement, 2) as department,
        count(distinct s.siret) as nb_active
    from months m
    join {{ source('sirene', 'silver') }} s
        on s.etatAdministratifEtablissement = 'A'
        and s.start_at < m.month_start
        and (s.end_at > m.month_start or s.end_at is null)
    group by 1, 2, 3
),

creations as (
    select
        date_trunc('month', dateCreationEtablissement) as month,
        activitePrincipaleEtablissement as ape_code,
        left(codeCommuneEtablissement, 2) as department,
        count(distinct siret) as nb_creations
    from {{ source('sirene', 'silver') }}
    where dateCreationEtablissement is not null
    group by 1, 2, 3
),

closures as (
    select
        date_trunc('month', start_at) as month,
        activitePrincipaleEtablissement as ape_code,
        left(codeCommuneEtablissement, 2) as department,
        count(distinct siret) as nb_closures
    from {{ source('sirene', 'silver') }}
    where etatAdministratifEtablissement = 'F'
    and start_at is not null
    group by 1, 2, 3
),

reopenings as (
    select
        date_trunc('month', start_at) as month,
        activitePrincipaleEtablissement as ape_code,
        left(codeCommuneEtablissement, 2) as department,
        count(distinct siret) as nb_reopenings
    from {{ source('sirene', 'silver') }}
    where etatAdministratifEtablissement = 'A'
    and start_at is not null
    and date_trunc('month', start_at) != date_trunc('month', dateCreationEtablissement)
    group by 1, 2, 3
)

select
    d.month,
    d.ape_code,
    d.department,
    coalesce(a.nb_active, 0) as nb_active,
    coalesce(c.nb_creations, 0) as nb_creations,
    coalesce(cl.nb_closures, 0) as nb_closures,
    coalesce(r.nb_reopenings, 0) as nb_reopenings
from dim d
left join active a on a.month = d.month and a.ape_code = d.ape_code and a.department = d.department
left join creations c on c.month = d.month and c.ape_code = d.ape_code and c.department = d.department
left join closures cl on cl.month = d.month and cl.ape_code = d.ape_code and cl.department = d.department
left join reopenings r on r.month = d.month and r.ape_code = d.ape_code and r.department = d.department
order by 1, 2, 3