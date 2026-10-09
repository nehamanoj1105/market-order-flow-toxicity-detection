from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    PROJECT_NAME: str = "Market Toxicity Dashboard API"

    PG_DSN: str = "postgresql://postgres:devpassword@postgres:5432/toxicflow"
    KAFKA_BROKERS: str = "kafka:9092"
    ALERTS_TOPIC: str = "trades.alerts"

    # Authentication
    JWT_SECRET: str = "CHANGE_THIS_SECRET"
    JWT_ALGORITHM: str = "HS256"
    JWT_EXPIRE_MINUTES: int = 60

    ADMIN_USERNAME: str = "admin"
    ADMIN_PASSWORD: str = "password"

    class Config:
        env_file = ".env"


settings = Settings()
