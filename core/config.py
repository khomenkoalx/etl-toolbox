from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    db_connection_string_secret: str
    dadata_token: str
    dadata_secret: str
    email_sender: str
    email_password: str
    email_receiver: str
    smtp_host: str
    smtp_port: str

    model_config = SettingsConfigDict(env_file='.env')


settings = Settings()
