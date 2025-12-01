from abc import ABC, abstractmethod
from pathlib import Path
from src.utils.logger import get_logger

logger = get_logger(__name__)

class BasePipeline(ABC):

    @abstractmethod
    def extract(self, municipio: str, ano: int):
        pass

    @abstractmethod
    def transform(self, df):
        pass

    @abstractmethod
    def load(self, df, municipio: str, ano: int):
        pass

    def ensure_dirs(self, municipio: str, ano: int):
        base_raw = Path(f"data/raw/{municipio}/{ano}")
        base_staging = Path(f"data/staging/{municipio}/{ano}")
        base_processed = Path(f"data/processed/{municipio}/{ano}")

        base_raw.mkdir(parents=True, exist_ok=True)
        base_staging.mkdir(parents=True, exist_ok=True)
        base_processed.mkdir(parents=True, exist_ok=True)

        return base_raw, base_staging, base_processed

    def run(self, municipio: str, ano: int):
        logger.info(f"Iniciando pipeline: {self.__class__.__name__} | {municipio} | {ano}")

        raw_path, staging_path, processed_path = self.ensure_dirs(municipio, ano)

        df_raw = self.extract(municipio, ano)
        df_tr = self.transform(df_raw)
        self.load(df_tr, municipio, ano)

        logger.info(f"Pipeline finalizada: {self.__class__.__name__} | {municipio} | {ano}")