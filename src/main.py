
import os
import sys
from pathlib import Path

# Adicionar a raiz do projeto ao PYTHONPATH
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from src.etl.receita_etl import ReceitaPipeline
from src.etl.despesa_etl import DespesaPipeline

def run_pipelines():
    municipio = "taubate"
    ano = 2025

    print("\n=== EXECUTANDO PIPELINES DO ALETHEIA DATACORE ===\n")

    print("➡️ ReceitaPipeline...")
    ReceitaPipeline().run(municipio, ano)

    print("➡️ DespesaPipeline...")
    DespesaPipeline().run(municipio, ano)

    print("\n✔ Pipelines finalizadas com sucesso!\n")

if __name__ == "__main__":
    run_pipelines()

