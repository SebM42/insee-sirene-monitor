from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType, TimestampType, DoubleType
from dotenv import load_dotenv
import os
from pathlib import Path
load_dotenv(Path(__file__).parent.parent / ".env")

DATABRICKS_USER = os.environ["DATABRICKS_USER"]
DBT_PROJECT_DIR = f"/Workspace/Users/{DATABRICKS_USER}/insee-sirene-monitor/dbt"

DATABRICKS_SCOPE = "databricks-credentials"

INSEE_API_SCOPE = "insee-sirene-monitor-api-credentials"
INSEE_API_ENDPOINT = "https://api.insee.fr/api-sirene/3.11/siret"
FILTERED_DEPARTMENTS = ["01","03","07","15","26","38","42","43","63","69","73","74"]
STOCK_FILE_URL = "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockEtablissement_utf8.zip"
STOCK_HISTORY_FILE_URL = "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockEtablissementHistorique_utf8.zip"


VOLUME_PROJECT = 'workspace.sirene'
VOLUME_STOCK = f'{VOLUME_PROJECT}.stock'
VOLUME_STATE = f'{VOLUME_PROJECT}.state'
VOLUME_BRONZE_STAGING = f'{VOLUME_PROJECT}.bronze_staging'

STOCK_FILTERED_BASE_PARQUET_DIR = f"/Volumes/{VOLUME_STOCK.replace('.','/')}/parquet/base"
STOCK_FILTERED_HISTORY_PARQUET_DIR = f"/Volumes/{VOLUME_STOCK.replace('.','/')}/parquet/history"

STATE_PATH = f"/Volumes/{VOLUME_STATE.replace('.','/')}/"
PIPELINE_STATE_PATH = f"{STATE_PATH}pipeline_state.json"

BRONZE_STAGING_DIR = f"/Volumes/{VOLUME_BRONZE_STAGING.replace('.','/')}"

BRONZE_TABLE = "sirene.bronze"
SILVER_TABLE = "sirene.silver"
GOLD_TABLES = [
    "sirene.gold_sector_trends",
    "sirene.gold_regional_activity",
    "sirene.gold_active_establishments"
]

SIRENE_COLUMNS = {
    "siret": {
        "field": StructField("siret", StringType(), True),
        "comment": "Identifiant unique de l'établissement",
        "insee_historized": None,
        "project_historized": False,
        "source": True, # True = in the csv source file ; False = engineered by the pipeline
    },
    "dateCreationEtablissement": {
        "field": StructField("dateCreationEtablissement", DateType(), True),
        "comment": "Date de création de l'établissement",
        "insee_historized": None,
        "project_historized": False,
        "source": True, # True = in the csv source file ; False = engineered by the pipeline
    },
    "etatAdministratifEtablissement": {
        "field": StructField("etatAdministratifEtablissement", StringType(), True),
        "comment": "Etat administratif de l'établissement (A=Actif, F=Fermé)",
        "insee_historized": True,
        "project_historized": True,
        "source": True, # True = in the csv source file ; False = engineered by the pipeline
    },
    "activitePrincipaleEtablissement": {
        "field": StructField("activitePrincipaleEtablissement", StringType(), True),
        "comment": "Code APE en nomenclature NAFRev2",
        "insee_historized": True,
        "project_historized": True,
        "source": True, # True = in the csv source file ; False = engineered by the pipeline
    },
    "activitePrincipaleNAF25Etablissement": {
        "field": StructField("activitePrincipaleNAF25Etablissement", StringType(), True),
        "comment": "Code APE en nomenclature NAF25 - suit les changements de activitePrincipaleEtablissement",
        "insee_historized": False,
        "project_historized": True,
        "source": True, # True = in the csv source file ; False = engineered by the pipeline
    },
    "caractereEmployeurEtablissement": {
        "field": StructField("caractereEmployeurEtablissement", StringType(), True),
        "comment": "Caractère employeur de l'établissement (O=Oui, N=Non)",
        "insee_historized": True,
        "project_historized": True,
        "source": True, # True = in the csv source file ; False = engineered by the pipeline
    },
    "trancheEffectifsEtablissement": {
        "field": StructField("trancheEffectifsEtablissement", StringType(), True),
        "comment": "Tranche d'effectifs salariés de l'établissement",
        "insee_historized": False,
        "project_historized": True,
        "source": True, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "etablissementSiege": {
        "field": StructField("etablissementSiege", BooleanType(), True),
        "comment": "Indique si l'établissement est le siège de l'unité légale",
        "insee_historized": False,
        "project_historized": True,
        "source": True, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "codePostalEtablissement": {
        "field": StructField("codePostalEtablissement", StringType(), True),
        "comment": "Code postal de l'établissement",
        "insee_historized": False,
        "project_historized": False,
        "source": True, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "codeCommuneEtablissement": {
        "field": StructField("codeCommuneEtablissement", StringType(), True),
        "comment": "Code INSEE de la commune de l'établissement",
        "insee_historized": False,
        "project_historized": False,
        "source": True, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "libelleCommuneEtablissement": {
        "field": StructField("libelleCommuneEtablissement", StringType(), True),
        "comment": "Libellé de la commune de l'établissement",
        "insee_historized": False,
        "project_historized": False,
        "source": True, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "coordonneeLambertAbscisseEtablissement": {
        "field": StructField("coordonneeLambertAbscisseEtablissement", DoubleType(), True),
        "comment": "Coordonnée Lambert X - null si diffusion partielle ou non renseigné",
        "insee_historized": False,
        "project_historized": False,
        "source": True, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "coordonneeLambertOrdonneeEtablissement": {
        "field": StructField("coordonneeLambertOrdonneeEtablissement", DoubleType(), True),
        "comment": "Coordonnée Lambert Y - null si diffusion partielle ou non renseigné",
        "insee_historized": False,
        "project_historized": False,
        "source": True, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "nombrePeriodesEtablissement": {
        "field": StructField("nombrePeriodesEtablissement", StringType(), True),
        "comment": "Nombre de périodes historisées de l'établissement - non conservé dans Silver",
        "insee_historized": None,
        "project_historized": None,
        "source": True, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "dateDernierTraitementEtablissement": {
        "field": StructField("dateDernierTraitementEtablissement", TimestampType(), True),
        "comment": "Date de dernière modification d'un champ non historisé - non conservé dans Silver",
        "insee_historized": None,
        "project_historized": None,
        "source": True, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "dateDebut": {
        "field": StructField("dateDebut", DateType(), True),
        "comment": "Date de début de la période courante des champs historisés - non conservé dans Silver",
        "insee_historized": None,
        "project_historized": None,
        "source": True, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "dateFin": {
        "field": StructField("dateFin", DateType(), True),
        "comment": "Date de fin de la période - présente uniquement dans le fichier historique - non conservé dans Silver",
        "insee_historized": None,
        "project_historized": None,
        "source": False, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "batch_created": {
        "field": StructField("batch_created", DateType(), True),
        "comment": "Date du batch lors duquel la ligne a été créée dans le pipeline",
        "insee_historized": None,
        "project_historized": False,
        "source": False, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "batch_closed": {
        "field": StructField("batch_closed", DateType(), True),
        "comment": "Date du batch lors duquel la ligne a été fermée - null si période courante, batch_created si ligne déjà fermée au first_fetch",
        "insee_historized": None,
        "project_historized": False,
        "source": False, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "start_at": {
        "field": StructField("start_at", DateType(), True),
        "comment": "Date de début de la période Silver - GREATEST(dateDebut, dateDernierTraitementEtablissement) pour période unique, dateDebut pour historique",
        "insee_historized": None,
        "project_historized": False,
        "source": False, # True = présent dans le CSV source ; False = construit par le pipeline
    },
    "end_at": {
        "field": StructField("end_at", DateType(), True),
        "comment": "Date de fin de la période Silver - null si période courante",
        "insee_historized": None,
        "project_historized": False,
        "source": False, # True = présent dans le CSV source ; False = construit par le pipeline
    },
}

HISTORIZED_COLS = [
    name for name, col in SIRENE_COLUMNS.items() 
    if col["insee_historized"] is True
]
NON_HISTORIZED_COLS = [
    name for name, col in SIRENE_COLUMNS.items() 
    if col["insee_historized"] is False
]
PROJECT_HISTORIZED_COLS = [
    name for name, col in SIRENE_COLUMNS.items() 
    if col["project_historized"] is True
]
PROJECT_ONLY_HISTORIZED_COLS = [name for name, col in SIRENE_COLUMNS.items() 
                                  if col["insee_historized"] is False 
                                  and col["project_historized"] is True]
FIXED_COLS = [name for name, col in SIRENE_COLUMNS.items() 
              if col["project_historized"] is False 
              and col["source"] is True 
              and name != "siret"]
SILVER_COLS = [
    name for name, col in SIRENE_COLUMNS.items() 
    if col["project_historized"] is not None
]
SIRENE_INTERMEDIATE_SCHEMA = StructType([
    col["field"] for col in SIRENE_COLUMNS.values()
    if col["source"] is True
])
SILVER_COMMENTS = {
    name: data["comment"] for name, data in SIRENE_COLUMNS.items()
    if data["project_historized"] is not None
}