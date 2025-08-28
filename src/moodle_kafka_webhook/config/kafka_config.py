from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import SecretStr, field_validator
from dotenv import load_dotenv
import os

load_dotenv()

class KafkaConfig(BaseSettings):
    bootstrap_servers: str
    security_protocol: str = "SASL_SSL"
    sasl_mechanism: str = "PLAIN"
    sasl_plain_username: str
    sasl_plain_password: SecretStr
    topic_target: str = "moodle_events"

    @field_validator("bootstrap_servers", "sasl_plain_username", "sasl_plain_password")
    @classmethod
    def validate_kafka_fields(cls, v, info):
        if not v:
            raise ValueError(f"{info.field_name} is required!")
        return v

    model_config = SettingsConfigDict(env_prefix='kafka_', frozen=True)

kafka_config = KafkaConfig()