from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env', env_file_encoding='utf-8'
    )

    ACCOUNT_NAME_DL: str
    CREDENTIAL_DL: str
    LOCAL_DATA_PATH: str
    CONNECTION_STRING_DL: str
