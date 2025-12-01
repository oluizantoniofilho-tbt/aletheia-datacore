
from src.etl.receita_etl import ReceitaPipeline
from src.etl.despesa_etl import DespesaPipeline

def run_pipelines():
    municipio = "taubate"
    ano = 2025

    print("Executando ReceitaPipeline...")
    ReceitaPipeline().run(municipio, ano)

    print("Executando DespesaPipeline...")
    DespesaPipeline().run(municipio, ano)

if __name__ == "__main__":
    run_pipelines()
