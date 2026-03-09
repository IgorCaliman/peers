# ==========================================================================
# cvm_pipeline.py
# Pipeline automático: baixa dados CDA da CVM, gera parquets consolidados
# e atualiza o JSON de tickers válidos.
# ==========================================================================

import os
import io
import json
import zipfile
import logging
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

# --------------------------------------------------------------------------
# CONFIGURAÇÕES
# --------------------------------------------------------------------------

# URL base dos arquivos CDA mensais da CVM
# Formato: cda_fi_BLC_X_AAAAMM.csv (blocos 1 a 8) dentro do ZIP mensal
CVM_BASE_URL = "https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/"

# Colunas necessárias dos arquivos da CVM (reduz memória)
COLUNAS_NECESSARIAS = [
    "TP_FUNDO", "CNPJ_FUNDO", "DENOM_SOCIAL", "DT_COMPTC",
    "TP_APLIC", "TP_ATIVO", "CD_ATIVO", "VL_MERC_POS_FINAL",
    "DS_ATIVO",
]

# Blocos que contêm ações (BLC_3 = ações, BLC_4 = BDRs/outros)
# BLC_1=TVM, BLC_2=Cotas, BLC_3=Ações, BLC_4=Outros TVM, BLC_5=Direitos, etc.
BLOCOS_DE_INTERESSE = ["BLC_3", "BLC_4"]

# Tipos de aplicação e ativo que o final.py já usa
TIPOS_APLIC_ACOES = [
    "Ações",
    "Certificado ou recibo de depósito de valores mobiliários",
    "Ações e outros TVM cedidos em empréstimo",
]
TIPOS_ATIVO_ACAO = [
    "Ação ordinária", "Ação preferencial",
    "Certificado de depósito de ações", "Recibo de subscrição", "UNIT",
]

# Sufixos de ticker válidos (B3)
SUFIXOS_VALIDOS = tuple(str(i) for i in range(1, 12))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
log = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# FUNÇÕES AUXILIARES
# --------------------------------------------------------------------------

def _meses_disponiveis_na_cvm(n_meses_atras: int = 18) -> list[str]:
    """
    Retorna lista de strings 'AAAAMM' dos últimos n_meses_atras meses.
    A CVM publica com ~2 meses de atraso; filtramos na hora do download.
    """
    hoje = date.today()
    meses = []
    for i in range(n_meses_atras, 0, -1):
        d = hoje - relativedelta(months=i)
        meses.append(d.strftime("%Y%m"))
    return meses


def _url_zip_mensal(mes: str) -> str:
    """Monta a URL do ZIP mensal da CVM para um dado mês (AAAAMM)."""
    return f"{CVM_BASE_URL}cda_fi_{mes}.zip"


def _baixar_zip(url: str, timeout: int = 120) -> bytes | None:
    """Baixa um ZIP da CVM; retorna None se não existir (404) ou timeout."""
    try:
        resp = requests.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp.content
        elif resp.status_code == 404:
            return None
        else:
            log.warning(f"HTTP {resp.status_code} para {url}")
            return None
    except requests.RequestException as e:
        log.error(f"Erro ao baixar {url}: {e}")
        return None


def _ler_bloco_do_zip(zip_bytes: bytes, bloco: str, mes: str) -> pd.DataFrame | None:
    """
    Extrai e lê um CSV de bloco específico de dentro do ZIP da CVM.
    O nome do arquivo dentro do ZIP é: cda_fi_BLC_X_AAAAMM.csv
    """
    nome_csv = f"cda_fi_{bloco}_{mes}.csv"
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            if nome_csv not in zf.namelist():
                log.debug(f"  Arquivo {nome_csv} não encontrado no ZIP.")
                return None
            with zf.open(nome_csv) as f:
                df = pd.read_csv(
                    f,
                    sep=";",
                    encoding="ISO-8859-1",
                    dtype=str,          # lê tudo como string primeiro
                    low_memory=False,
                )
        # Mantém apenas colunas necessárias que existam no arquivo
        cols = [c for c in COLUNAS_NECESSARIAS if c in df.columns]
        return df[cols].copy()
    except Exception as e:
        log.error(f"Erro ao ler {nome_csv}: {e}")
        return None


def _limpar_ticker(ticker: str) -> str | None:
    """
    Valida e limpa um ticker da B3.
    Regra: 4 letras maiúsculas + 1-2 dígitos (ex: PETR4, ITUB3, ABCD11).
    Retorna None se inválido.
    """
    if not isinstance(ticker, str):
        return None
    t = ticker.strip().upper()
    # Remove sufixo .SA se presente
    if t.endswith(".SA"):
        t = t[:-3]
    # Validação básica: 5 ou 6 caracteres, começa com 4 letras
    if len(t) in (5, 6) and t[:4].isalpha() and t[4:].isdigit():
        return t
    return None


# --------------------------------------------------------------------------
# FUNÇÃO PRINCIPAL
# --------------------------------------------------------------------------

def executar_pipeline(
    pasta_saida: str = ".",
    cnpjs_interesse: list[str] | None = None,
    n_meses: int = 12,
    forcar_reprocessamento: bool = False,
) -> list[str]:
    """
    Baixa dados CDA da CVM, gera arquivos `carteira_consolidada_AAAAMM.parquet`
    e atualiza `tickers_unicos_limpos.json`.

    Parâmetros
    ----------
    pasta_saida : str
        Diretório onde os parquets e o JSON serão salvos.
    cnpjs_interesse : list[str] | None
        Se fornecido, filtra apenas os fundos com esses CNPJs.
        Se None, mantém todos os fundos (arquivo pode ser grande).
    n_meses : int
        Quantos meses retroativos verificar na CVM (padrão: 12).
    forcar_reprocessamento : bool
        Se True, recria os parquets mesmo que já existam.

    Retorna
    -------
    list[str]
        Lista de strings 'AAAAMM' dos meses processados com sucesso.
    """
    pasta_saida = Path(pasta_saida)
    pasta_saida.mkdir(parents=True, exist_ok=True)

    meses_candidatos = _meses_disponiveis_na_cvm(n_meses)
    meses_processados = []
    todos_tickers = set()

    # Carrega tickers já existentes (para não perder meses antigos)
    arquivo_tickers = pasta_saida / "tickers_unicos_limpos.json"
    if arquivo_tickers.exists():
        with open(arquivo_tickers, "r") as f:
            todos_tickers = set(json.load(f))

    for mes in meses_candidatos:
        arquivo_parquet = pasta_saida / f"carteira_consolidada_{mes}.parquet"

        if arquivo_parquet.exists() and not forcar_reprocessamento:
            log.info(f"[{mes}] Parquet já existe. Pulando.")
            # Extrai tickers do parquet existente
            df_existente = pd.read_parquet(arquivo_parquet, columns=["CD_ATIVO"])
            todos_tickers.update(df_existente["CD_ATIVO"].dropna().unique())
            meses_processados.append(mes)
            continue

        log.info(f"[{mes}] Baixando dados da CVM...")
        url = _url_zip_mensal(mes)
        zip_bytes = _baixar_zip(url)

        if zip_bytes is None:
            log.warning(f"[{mes}] Dados não disponíveis na CVM ainda. Pulando.")
            continue

        # Lê e concatena os blocos de interesse
        blocos = []
        for bloco in BLOCOS_DE_INTERESSE:
            df_bloco = _ler_bloco_do_zip(zip_bytes, bloco, mes)
            if df_bloco is not None:
                blocos.append(df_bloco)

        if not blocos:
            log.warning(f"[{mes}] Nenhum bloco válido encontrado no ZIP.")
            continue

        df_mes = pd.concat(blocos, ignore_index=True)

        # Filtra por CNPJ se fornecido
        if cnpjs_interesse and "CNPJ_FUNDO" in df_mes.columns:
            df_mes = df_mes[df_mes["CNPJ_FUNDO"].isin(cnpjs_interesse)].copy()

        # Converte tipos
        if "VL_MERC_POS_FINAL" in df_mes.columns:
            df_mes["VL_MERC_POS_FINAL"] = (
                df_mes["VL_MERC_POS_FINAL"]
                .str.replace(",", ".", regex=False)
                .pipe(pd.to_numeric, errors="coerce")
            )
        if "DT_COMPTC" in df_mes.columns:
            df_mes["DT_COMPTC"] = pd.to_datetime(df_mes["DT_COMPTC"], errors="coerce")

        # Coleta tickers válidos
        if "CD_ATIVO" in df_mes.columns:
            tickers_mes = {
                t for t in df_mes["CD_ATIVO"].dropna().unique()
                if _limpar_ticker(t) is not None
            }
            todos_tickers.update(tickers_mes)

        # Salva parquet
        df_mes.to_parquet(arquivo_parquet, index=False)
        log.info(f"[{mes}] Salvo: {arquivo_parquet.name}  ({len(df_mes):,} linhas)")
        meses_processados.append(mes)

    # Salva JSON de tickers válidos atualizado
    tickers_limpos = sorted(
        t for t in todos_tickers if _limpar_ticker(t) is not None
    )
    with open(arquivo_tickers, "w") as f:
        json.dump(tickers_limpos, f, ensure_ascii=False)
    log.info(f"tickers_unicos_limpos.json atualizado: {len(tickers_limpos)} tickers.")

    return sorted(meses_processados)


# --------------------------------------------------------------------------
# USO STANDALONE (python cvm_pipeline.py)
# --------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline CVM – baixa e processa carteiras CDA.")
    parser.add_argument("--pasta", default=".", help="Pasta de saída dos parquets.")
    parser.add_argument("--meses", type=int, default=12, help="Quantos meses retroativos checar.")
    parser.add_argument("--forcar", action="store_true", help="Reprocessar mesmo se parquet já existe.")
    args = parser.parse_args()

    meses = executar_pipeline(
        pasta_saida=args.pasta,
        n_meses=args.meses,
        forcar_reprocessamento=args.forcar,
    )
    print(f"\nMeses processados: {meses}")
