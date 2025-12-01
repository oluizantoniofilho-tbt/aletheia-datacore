
import pandas as pd
from pathlib import Path
from src.etl.base_pipeline import BasePipeline
from src.utils.logger import get_logger

logger = get_logger(__name__)

class ReceitaPipeline(BasePipeline):
    domain = "receitas"

    def extract(self, municipio: str, ano: int):
        file_path = Path(f"data/raw/{municipio}/{ano}/receitas.csv")

        if not file_path.exists():
            raise FileNotFoundError(f"Arquivo de receitas não encontrado: {file_path}")

        logger.info(f"Lendo arquivo: {file_path}")
        df = pd.read_csv(file_path, sep=",", encoding="utf-8")
        return df

    def transform(self, df: pd.DataFrame):
        logger.info("Iniciando transformação dos dados de receita")

        # Normalizar nomes das colunas
        df.columns = (
            df.columns.str.lower()
                        .str.strip()
                        .str.replace(" ", "_")
                        .str.replace("ã", "a")
                        .str.replace("á", "a")
                        .str.replace("â", "a")
                        .str.replace("ç", "c")
                        .str.replace("é", "e")
                        .str.replace("ê", "e")
                        .str.replace("í", "i")
                        .str.replace("/", "_")
        )

        # Converter valores para número
        if "valor" in df.columns:
            df["valor"] = pd.to_numeric(df["valor"], errors="coerce").fillna(0)

        # Garantir colunas essenciais
        if "ano" not in df.columns:
            df["ano"] = None

        if "municipio" not in df.columns:
            df["municipio"] = None

        return df

    def load(self, df: pd.DataFrame, municipio: str, ano: int):
        output_path = Path(f"data/processed/{municipio}/{ano}/receitas.parquet")

        logger.info(f"Salvando arquivo processado em: {output_path}")
        df.to_parquet(output_path, index=False)
