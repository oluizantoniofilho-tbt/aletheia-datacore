
import pandas as pd
from pathlib import Path
from src.etl.base_pipeline import BasePipeline
from src.utils.logger import get_logger

logger = get_logger(__name__)

class DespesaPipeline(BasePipeline):
    domain = "despesas"

    def extract(self, municipio: str, ano: int):
        file_path = Path(f"data/raw/{municipio}/{ano}/despesas.csv")

        if not file_path.exists():
            raise FileNotFoundError(f"Arquivo de despesas não encontrado: {file_path}")

        logger.info(f"Lendo arquivo: {file_path}")
        df = pd.read_csv(file_path, sep=",", encoding="utf-8")
        return df

    def transform(self, df: pd.DataFrame):
        logger.info("Iniciando transformação dos dados de despesa")

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

        # Converter valores
        col_valor = None
        for col in df.columns:
            if col.startswith("valor") or "valor" in col:
                col_valor = col
                break

        if col_valor:
            df[col_valor] = pd.to_numeric(df[col_valor], errors="coerce").fillna(0)

        # Garantir colunas essenciais
        if "ano" not in df.columns:
            df["ano"] = None

        if "municipio" not in df.columns:
            df["municipio"] = None

        return df

    def load(self, df: pd.DataFrame, municipio: str, ano: int):
        output_path = Path(f"data/processed/{municipio}/{ano}/despesas.parquet")

        logger.info(f"Salvando arquivo processado em: {output_path}")
        df.to_parquet(output_path, index=False)
