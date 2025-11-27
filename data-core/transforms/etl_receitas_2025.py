"""
ETL DE RECEITAS 2025 – VERSÃO ESTÁVEL
SEM NAN, SEM ERROS DE EXECUÇÃO
"""

import pandas as pd
from pathlib import Path

try:
    from .utils_io import RAW_DIR, load_table, save_json
except ImportError:
    from utils_io import RAW_DIR, load_table, save_json


# ----------------------------
# Limpeza de valores monetários
# ----------------------------
def clean_money(value):
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    v = str(value).strip()
    v = v.replace(".", "").replace(",", ".")
    try:
        return float(v)
    except:
        return None


# ----------------------------
# Execução principal
# ----------------------------
def run_etl_receitas_2025():
    print("\n=== INICIANDO ETL RECEITAS 2025 ===\n")

    # Encontrar arquivo
    target_file = None
    for f in RAW_DIR.glob("**/*"):
        if "receit" in f.name.lower() and "2025" in f.name.lower():
            target_file = f
            break

    if not target_file:
        raise FileNotFoundError("Arquivo de receitas 2025 não encontrado.")

    print(f"✓ Arquivo encontrado: {target_file}")

    df = load_table(target_file)
    print(f"✓ Linhas lidas: {len(df)}\n")

    print("Colunas originais:")
    print(list(df.columns))
    print("------------------------------------")

    # Renomear colunas
    column_mapping = {
        "Descrição": "descricao",
        "Cód. Contabil": "codigo_contabil",
        "Fonte": "fonte_recurso",
        "Órgão": "orgao",
        "Rubrica": "rubrica",
        "Aplicação": "aplicacao",
        "Valor Arrecadado": "valor_arrecadado",
        "Valor Orçado": "valor_orcado",
        "Valor Orçado Atualizado": "valor_orcado_atualizado",
        "Valor Creditado": "valor_creditado",
        "Valor Debitado": "valor_debitado",
    }

    df.rename(
        columns={k: v for k, v in column_mapping.items() if k in df.columns},
        inplace=True
    )

    # Remover linhas completamente vazias
    df.dropna(how="all", inplace=True)

    # 🔥 REMOVER LINHAS LIXO: descrição numérica, ou descrição None/NaN
    df["descricao_str"] = df["descricao"].astype(str).str.strip()

    df = df[~df["descricao_str"].str.fullmatch(r"\d+")]        # remove "1", "2", "3" etc
    df = df[df["descricao_str"].str.lower() != "nan"]          # remove NaN textual
    df = df[df["descricao_str"] != ""]                         # remove vazios

    df.drop(columns=["descricao_str"], inplace=True)

    # Converter valores monetários
    money_cols = [
        "valor_orcado",
        "valor_arrecadado",
        "valor_orcado_atualizado",
        "valor_creditado",
        "valor_debitado",
    ]

    for col in money_cols:
        if col in df.columns:
            df[col] = df[col].apply(clean_money)

    # Marcar linha TOTAL
    df["is_total"] = df["descricao"].astype(str).str.contains("total", case=False, na=False)

    # Substituir NaN por None
    df = df.where(pd.notnull(df), None)

    # 🔥 Validação final
    non_total = df[df["is_total"] == False]

    problematic = non_total[non_total.isna().any(axis=1)]
    if len(problematic) > 0:
        print("\n❌ Linhas problemáticas (não-total com None):")
        print(problematic)
        raise ValueError("Ainda há linhas não-total inválidas após limpeza.")

    # Salvar JSON
    save_json(df.to_dict(orient="records"), "receitas_2025.json")

    print("\n=== ETL CONCLUÍDO COM SUCESSO ===\n")


# EXECUÇÃO DIRETA
if __name__ == "__main__":
    run_etl_receitas_2025()
