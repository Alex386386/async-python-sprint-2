from pydantic_settings import BaseSettings


class SchedulerConfig(BaseSettings):
    pool_size: int = 10  # Значение по умолчанию
    statement_path: str = 'scheduler_state.json'

    class Config:
        env_file = ".env"
