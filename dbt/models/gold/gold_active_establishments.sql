{{
    config(
        materialized='table',
        schema='sirene'
    )
}}

select
    siret,
    dateCreationEtablissement,
    activitePrincipaleEtablissement,
    activitePrincipaleNAF25Etablissement,
    caractereEmployeurEtablissement,
    trancheEffectifsEtablissement,
    etablissementSiege,
    codePostalEtablissement,
    codeCommuneEtablissement,
    libelleCommuneEtablissement,
    left(codeCommuneEtablissement, 2) as departement,
    coordonneeLambertAbscisseEtablissement,
    coordonneeLambertOrdonneeEtablissement,
    start_at
from {{ source('sirene', 'silver') }}
where etatAdministratifEtablissement = 'A'
and end_at is null