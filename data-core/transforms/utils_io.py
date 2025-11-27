"""
Utilitários de I/O para o DataCore.

Funções para listar arquivos brutos, carregar tabelas (CSV/Excel)
e salvar dados em formato JSON no diretório de saída.
"""
import json
import pandas as pd
from pathlib import Path


# --- Diretórios Base -----------------------------------------

# BASE_DIR = raiz do projeto (pasta aletheia-datacore)
BASE_DIR = Path(__file__).resolve().parents[2]

# Aqui ficam os arquivos brutos
RAW_DIR = BASE_DIR / "data-core" / "raw"

# Aqui salvamos os JSON transformados
OUTPUT_DIR = BASE_DIR / "data-core" / "transforms" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# --- Funções --------------------------------------------------

def list_raw_files():
    """
    Lista todos os arquivos recursivamente dentro de data-core/raw.
    """
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Diretório de dados brutos não encontrado: {RAW_DIR}")
    return RAW_DIR.glob("**/*")


def load_table(path: Path) -> pd.DataFrame:
    """
    Carrega um arquivo CSV ou Excel em um DataFrame pandas.
    """
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    suffix = path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(path, sep=";", engine="python")

    if suffix in [".xlsx", ".xls"]:
        return pd.read_excel(path)

    raise ValueError(f"Tipo de arquivo não suportado: {suffix}")


def save_json(data, output_name: str):
    """
    Salva dados como JSON válido em data-core/transforms/output.
    """
    output_path = OUTPUT_DIR / output_name
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Dados salvos com sucesso em: {output_path}")
    return output_path


