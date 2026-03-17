BUCKET = "insee-sirene-monitor-data-landing-zone"
PIPELINE_STATE_KEY = "pipeline_state.json"
BATCH_STAGE_KEY = "batch_stage.json"
R2_SCOPE = "r2-insee-sirene-monitor-dlz-credentials"
INSEE_API_SCOPE = "insee-sirene-monitor-api-credentials"
INSEE_API_ENDPOINT = "https://api.insee.fr/api-sirene/3.11/siret"
AURA_DEPARTMENTS = ["01","03","07","15","26","38","42","43","63","69","73","74"]

BRONZE_TABLE = "insee_sirene.bronze"
SILVER_TABLE = "insee_sirene.silver"
GOLD_TABLES = [
    "insee_sirene.gold_sector_trends",
    "insee_sirene.gold_regional_activity"
]