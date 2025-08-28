from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator
from dotenv import load_dotenv
import os

load_dotenv()

class AppSettings(BaseSettings):
    webhook_token: str
    debug: bool = False

    @field_validator("webhook_token")
    @classmethod
    def webhook_token_must_not_be_empty(cls, v, info):
        if not v:
            raise ValueError("Webhook token cannot be empty")
        return v

    model_config = SettingsConfigDict(env_prefix='app_', frozen=True)

app_settings = AppSettings()