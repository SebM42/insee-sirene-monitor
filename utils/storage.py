from utils.config import INSEE_API_SCOPE


def get_insee_api_key(dbutils) -> str:
    """Retrieve the INSEE SIRENE API key from the Databricks secret vault."""
    
    return dbutils.secrets.get(
        scope=INSEE_API_SCOPE,
        key="api_key"
    )