from pydantic import BaseSettings
import os
print(os.listdir('./'))


class Settings(BaseSettings):
    PROJECT_NAME: str
    API_V1_STR: str
    SQLALCHEMY_WAREHOUSE_URI: str

    class Config:
        case_sensitive = True
        env_file = ".env"


class Production(Settings):
    class Config:
        env_file = '.production.env'


class Testing(Settings):
    class Config:
        env_file = '.testing.env'


def get_settings(env):
    if env == 'PRODUCTION':
        return Production()
    return Testing()


settings = get_settings('TESTING')
