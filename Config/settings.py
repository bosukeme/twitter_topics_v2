from decouple import config as env_config

MONGO_URL = env_config("MONGO_URL")