from typing import Annotated, ClassVar
from pydantic import BeforeValidator, SecretStr
from typing_extensions import override
from pydantic_settings import (
    BaseSettings,
    JsonConfigSettingsSource,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)


def convert_str2int(v: str | int):
    if isinstance(v, str):
        v = int(v)
    return v


IntMapStr = Annotated[int, BeforeValidator(convert_str2int)]


class ExternalMinioSettings(BaseSettings):
    ca_cert: str
    endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: SecretStr
    region_name: str
    logging_conf_file: str

    model_config: ClassVar[SettingsConfigDict] = SettingsConfigDict(
        json_file=("s3_config.json",),
    )

    @classmethod
    @override
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            env_settings,
            JsonConfigSettingsSource(settings_cls),
            dotenv_settings,
            file_secret_settings,
        )
