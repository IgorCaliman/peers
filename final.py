# ==========================================================================
# PARTE 1: IMPORTAÇÕES E CONFIGURAÇÕES GLOBAIS
# ==========================================================================
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import locale
import json
import numpy as np
import plotly.graph_objects as go
import yfinance as yf
import matplotlib.pyplot as plt
from datetime import date, datetime
from dateutil.relativedelta import relativedelta

# --- CONFIGURAÇÕES DE LOCALIZAÇÃO E PÁGINA ---
# ADICIONE ESTA NOVA FUNÇÃO NA PARTE 2

st.set_page_config(layout="wide", page_title="Análise Consolidada de Carteiras")
st.title('📊 Dashboard: Análise de Gestoras e Ativos')

# --- CONSTANTES E NOMES DE ARQUIVOS ---
CAMINHO_DA_PASTA = "."
PASTA_DADOS_FILTRADOS = "dados_filtrados"
NOME_ARQUIVO_LISTA_FUNDOS = "lista_completa_fundos_para_analise.xlsx"
ARQUIVO_TICKERS_LIMPOS = "tickers_unicos_limpos.json"

# ALTERADO: Centralizamos os dados de mercado em um único arquivo
ARQUIVO_ECONOMATICA_XLSX = "economatica.xlsx"

# Meses para análise e paleta de cores para os gráficos
MESES_PARA_ANALISE = ['202410','202411','202412','202501','202502','202503','202504', '202505', '202506', '202507', '202508']
PALETA_DE_CORES = ['#B0B8D1', '#5A76A8', '#001D6E']


# ==========================================================================
# PARTE 2: FUNÇÕES DE CARREGAMENTO DE DADOS
# ==========================================================================

@st.cache_data
def carregar_mapeamento_gestora_fundo(caminho_arquivo_excel):
    """Lê o arquivo de mapeamento Gestora -> Fundo."""
    if not os.path.exists(caminho_arquivo_excel):
        st.error(f"ERRO: O arquivo de mapeamento '{caminho_arquivo_excel}' não foi encontrado.");
        return None
    try:
        df_mapa = pd.read_excel(caminho_arquivo_excel)
        return df_mapa[['Gestora', 'Fundo']].dropna().drop_duplicates()
    except Exception as e:
        st.error(f"Ocorreu um erro ao ler o arquivo Excel de mapeamento: {e}");
        return None


@st.cache_data
def carregar_dados_historicos(caminho_da_pasta, meses):
    """Carrega os arquivos Parquet mensais JÁ CONSOLIDADOS."""
    lista_dfs_completos = []
    for mes in meses:
        try:
            path_consolidado = os.path.join(caminho_da_pasta, f'carteira_consolidada_{mes}.parquet')
            df_mes = pd.read_parquet(path_consolidado)
            lista_dfs_completos.append(df_mes)
        except FileNotFoundError:
            st.error(f"Arquivo consolidado para o mês {mes} não encontrado: '{path_consolidado}'.");
            st.stop()
    return pd.concat(lista_dfs_completos, ignore_index=True) if lista_dfs_completos else None


# NOVO: Função genérica para ler market cap, liquidez ou qualquer outro dado no mesmo formato.
@st.cache_data
def carregar_e_processar_planilha_wide(caminho_arquivo, nome_planilha, nome_coluna_valor):
    """
    Lê uma planilha específica de um arquivo Excel, transforma de formato 'wide' para 'long'
    e prepara para o merge.
    """
    try:
        df_wide = pd.read_excel(caminho_arquivo, sheet_name=nome_planilha)
        ticker_column_name = df_wide.columns[0]

        # Filtra apenas colunas cujo cabeçalho já é datetime (ignora colunas como "D0")
        colunas_data = [c for c in df_wide.columns[1:] if isinstance(c, (pd.Timestamp, datetime))]
        df_wide = df_wide[[ticker_column_name] + colunas_data]

        df_long = df_wide.melt(
            id_vars=[ticker_column_name],
            var_name='Data',
            value_name=nome_coluna_valor
        )

        df_long.rename(columns={ticker_column_name: 'Ticker'}, inplace=True)
        df_long[nome_coluna_valor] = pd.to_numeric(df_long[nome_coluna_valor], errors='coerce')
        df_long['Data'] = pd.to_datetime(df_long['Data'], dayfirst=True,
                                         errors='coerce')  # dayfirst=True para formato DD/MM/AAAA
        df_long.dropna(subset=['Data', nome_coluna_valor], inplace=True)
        df_long['MesAno'] = df_long['Data'].dt.strftime('%Y%m')

        return df_long[['Ticker', 'MesAno', nome_coluna_valor]]

    except FileNotFoundError:
        st.error(f"Arquivo de dados de mercado '{caminho_arquivo}' não encontrado.")
        st.stop()
    except ValueError as e:
        if f"Worksheet named '{nome_planilha}' not found" in str(e):
            st.error(f"Erro: A planilha '{nome_planilha}' não foi encontrada no arquivo '{caminho_arquivo}'.")
            st.stop()
        else:
            st.error(f"Erro de valor ao processar '{nome_planilha}': {e}");
            st.stop()
    except Exception as e:
        st.error(f"Erro desconhecido ao processar a planilha '{nome_planilha}': {e}");
        st.stop()


def formatar_moeda_brl(valor):
    """Formata um número como moeda no padrão BRL (R$ 1.234,56)."""
    if pd.isna(valor):
        return "N/A"
    # Formata com vírgula de milhar e duas casas decimais, usando placeholders
    valor_formatado = f'{valor:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {valor_formatado}"


def plotar_ratio(t1, t2, start, end):
    """
    Baixa os dados de dois tickers, calcula a razão entre eles e plota o gráfico.
    """
    # Garante que o sufixo .SA (B3) seja adicionado se não estiver presente
    t1_full = t1 if t1.upper().endswith('.SA') else f'{t1}.SA'
    t2_full = t2 if t2.upper().endswith('.SA') else f'{t2}.SA'

    # Baixa apenas a coluna 'Close' e remove linhas com dados faltantes
    df = yf.download([t1_full, t2_full], start=start, end=end)['Close'].dropna()

    # Verifica se os dados para ambos os tickers foram retornados
    if df.empty or len(df.columns) < 2:
        st.warning("Não foi possível obter dados para ambos os tickers no período selecionado.")
        return

    # Calcula a razão e as estatísticas
    ratio = df[t1_full] / df[t2_full]
    media = ratio.mean()
    std = ratio.std()

    # Cria o gráfico usando Matplotlib
    fig, ax = plt.subplots(figsize=(14, 6))
    ax.plot(ratio.index, ratio, label=f'Razão {t1}/{t2}')
    ax.axhline(media, color='red', linestyle='--', label=f'Média ({media:.2f})')
    ax.axhline(media + std, color='orange', linestyle=':', label=f'+1σ ({media + std:.2f})')
    ax.axhline(media - std, color='orange', linestyle=':', label=f'-1σ ({media - std:.2f})')

    # Configurações visuais do gráfico
    ax.set_title(f'Razão entre {t1} e {t2}')
    ax.set_xlabel('Data')
    ax.set_ylabel('Ratio')
    ax.grid(True, linestyle='--', alpha=0.6)
    ax.legend()

    # Exibe o gráfico no Streamlit
    st.pyplot(fig)


# ==========================================================================
# PARTE 3: LÓGICA PRINCIPAL E PROCESSAMENTO DE DADOS
# ==========================================================================

# --- Carregamento Inicial ---

mapa_gestora_fundo = carregar_mapeamento_gestora_fundo(os.path.join(CAMINHO_DA_PASTA, NOME_ARQUIVO_LISTA_FUNDOS))
dados_brutos = carregar_dados_historicos(CAMINHO_DA_PASTA, MESES_PARA_ANALISE)

df_market_caps = carregar_e_processar_planilha_wide(
    os.path.join(CAMINHO_DA_PASTA, ARQUIVO_ECONOMATICA_XLSX),
    'marketcap',
    'Market_Cap_Cia_R'
)
df_market_caps['Market_Cap_Cia_R'] = df_market_caps['Market_Cap_Cia_R'] * 1000
df_liquidez = carregar_e_processar_planilha_wide(
    os.path.join(CAMINHO_DA_PASTA, ARQUIVO_ECONOMATICA_XLSX),
    'liquidez',
    'Volume_Medio_Financeiro_60d'
)

# Avisa se algum mês da análise está fora da cobertura do economatica.xlsx
meses_economatica = set(df_market_caps['MesAno'].unique())
meses_faltando = [m for m in MESES_PARA_ANALISE if m not in meses_economatica]
if meses_faltando:
    st.warning(
        f"⚠️ Os seguintes meses estão em MESES_PARA_ANALISE mas **não têm cobertura** "
        f"no economatica.xlsx (colunas sem data válida no cabeçalho): "
        f"**{', '.join(meses_faltando)}**. "
        f"Market Cap e Liquidez aparecerão como N/A para esses meses. "
        f"Corrija o cabeçalho da planilha ou remova esses meses de MESES_PARA_ANALISE."
    )

if mapa_gestora_fundo is None or dados_brutos is None or df_market_caps is None or df_liquidez is None:
    st.error("Falha no carregamento de um dos arquivos base. O dashboard não pode continuar.");
    st.stop()

try:
    with open(ARQUIVO_TICKERS_LIMPOS, 'r') as f:
        tickers_validos = json.load(f)
    # st.sidebar.success(f"{len(tickers_validos)} tickers válidos carregados.")
except FileNotFoundError:
    st.error(f"ERRO: Arquivo de tickers limpos '{ARQUIVO_TICKERS_LIMPOS}' não foi encontrado!");
    st.stop()

# --- Processamento e Consolidação ---
dados_completos = pd.merge(dados_brutos, mapa_gestora_fundo, left_on='DENOM_SOCIAL', right_on='Fundo', how='inner')
dados_completos['VL_MERC_POS_FINAL'] = pd.to_numeric(dados_completos['VL_MERC_POS_FINAL'], errors='coerce')

tipos_aplic_interesse_acoes = ['Ações', 'Certificado ou recibo de depósito de valores mobiliários',
                               'Ações e outros TVM cedidos em empréstimo']
tipos_ativo_acao = ['Ação ordinária', 'Ação preferencial', 'Certificado de depósito de ações', 'Recibo de subscrição',
                    'UNIT']
dados_acoes = dados_completos[(dados_completos['TP_APLIC'].isin(tipos_aplic_interesse_acoes)) & (
    dados_completos['TP_ATIVO'].isin(tipos_ativo_acao))].copy()
dados_acoes.dropna(subset=['CD_ATIVO'], inplace=True)

registros_antes = len(dados_acoes)
dados_acoes = dados_acoes[dados_acoes['CD_ATIVO'].isin(tickers_validos)].copy()
registros_depois = len(dados_acoes)

posicao_consolidada = dados_acoes.groupby(['DT_COMPTC', 'Gestora', 'CD_ATIVO'], as_index=False).agg(
    Valor_Consolidado_R=('VL_MERC_POS_FINAL', 'sum'))
posicao_consolidada['MesAno'] = pd.to_datetime(posicao_consolidada['DT_COMPTC']).dt.strftime('%Y%m')

# Merge com Market Cap e Liquidez para criar o DataFrame final
df_final = pd.merge(posicao_consolidada, df_market_caps, left_on=['CD_ATIVO', 'MesAno'], right_on=['Ticker', 'MesAno'],
                    how='left')
df_final = pd.merge(df_final, df_liquidez, left_on=['CD_ATIVO', 'MesAno'], right_on=['Ticker', 'MesAno'], how='left')
df_final.rename(columns={"CD_ATIVO": "Ativo"}, inplace=True)
df_final.drop(columns=[col for col in df_final.columns if 'Ticker' in col], inplace=True)

if df_final.empty:
    st.warning("Após os filtros, nenhum dado de ação foi encontrado para a lista de fundos e meses selecionados.");
    st.stop()

# --- Cálculos Finais ---
df_final['Perc_Cia'] = (df_final['Valor_Consolidado_R'] / df_final['Market_Cap_Cia_R']) * 100
df_final['PL_Total_Gestora_Mes'] = df_final.groupby(['Gestora', 'DT_COMPTC'])['Valor_Consolidado_R'].transform('sum')

# ==========================================================================
# PARTE 4: ABA 1 - ANÁLISE POR GESTORA
# ==========================================================================
# =======================================================================
# NAVEGAÇÃO + PLACEHOLDERS DA SIDEBAR
# =======================================================================
nav_container     = st.sidebar.container()      # seletor de página (topo)
filtros_container = st.sidebar.container()      # filtros que mudam
paginas = ["Análise por gestora",
           "Análise por ativo",
           "Movimentações relevantes",
           "Razão tickers",
           "Overlap entre gestoras",
           "Pressão de liquidez"]
pagina  = nav_container.selectbox("Página:", paginas)

sidebar = filtros_container.empty()             # placeholder dos filtros

# =======================================================================
# PÁGINA 1 – ANÁLISE POR GESTORA
# =======================================================================
if pagina == "Análise por gestora":

    # --------- FILTRO EXCLUSIVO (fica só na sidebar) ---------
    with sidebar.container():
        st.header("Filtro por Gestora")
        lista_gestoras = sorted(df_final['Gestora'].unique())
        if not lista_gestoras:
            st.warning("Nenhuma gestora encontrada nos dados processados.")
            st.stop()            # interrompe a página – não há dados
        gestora_sel = st.selectbox(
            "Selecione a Gestora:",
            lista_gestoras,
            key="f_gestora"
        )

    # --------- CONTEÚDO PRINCIPAL (painel central) ----------
    st.header("Análise por Gestora", divider="blue")

    dados_gestora = df_final[df_final["Gestora"] == gestora_sel].copy()
    datas_disp    = sorted(dados_gestora["DT_COMPTC"].unique(), reverse=True)
    if not datas_disp:
        st.warning(f"Nenhum dado de ações encontrado para a gestora {gestora_sel}.")
        st.stop()

    mes_sel = datas_disp[0]
    st.subheader(f"Visão Consolidada - {pd.to_datetime(mes_sel).strftime('%B de %Y')}",
                 divider="blue")
    dados_mes = dados_gestora[dados_gestora["DT_COMPTC"] == mes_sel].copy()

    pl_gestora = dados_mes["Valor_Consolidado_R"].sum()
    dados_mes["Perc_PL"] = (
        dados_mes["Valor_Consolidado_R"] / pl_gestora * 100
        if pl_gestora else 0
    )

    # ---------- calculando liquidez opcional ----------
    if "Volume_Medio_Financeiro_60d" in dados_mes.columns:
        liquidez_ok = (
            dados_mes["Volume_Medio_Financeiro_60d"].notna() &
            (dados_mes["Volume_Medio_Financeiro_60d"] > 0)
        )
        dados_mes.loc[liquidez_ok, "Dias_Zerar_20"] = (
            dados_mes.loc[liquidez_ok, "Valor_Consolidado_R"] /
            (0.20 * dados_mes.loc[liquidez_ok, "Volume_Medio_Financeiro_60d"])
        ) / 1000
        dados_mes.loc[liquidez_ok, "Dias_Zerar_30"] = (
            dados_mes.loc[liquidez_ok, "Valor_Consolidado_R"] /
            (0.30 * dados_mes.loc[liquidez_ok, "Volume_Medio_Financeiro_60d"])
        ) / 1000

    # ---------- métricas de topo ----------
    col1, col2 = st.columns(2)
    col1.metric("PL em Ações (Consolidado)", f"R$ {pl_gestora:,.2f}")
    col2.metric("Nº de Ativos na Carteira",  f"{len(dados_mes)}")

    # ---------- tabela de posições ----------
    st.subheader("Exposição Total em Ações (Consolidado)")
    tabela = dados_mes.sort_values("Perc_PL", ascending=False).copy()
    tabela["Valor (R$)"]       = tabela["Valor_Consolidado_R"].apply(formatar_moeda_brl)
    tabela["Market Cap (R$)"]  = tabela["Market_Cap_Cia_R"].apply(formatar_moeda_brl)
    tabela["% do PL"]          = tabela["Perc_PL"].apply(lambda x: f"{x:.2f}%")
    tabela["% da Cia"]         = tabela["Perc_Cia"].apply(
        lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A"
    )
    cols = ["Ativo", "Valor (R$)", "% do PL", "Market Cap (R$)", "% da Cia"]
    if "Dias_Zerar_20" in tabela.columns:
        tabela["Dias p/ Zerar (20% Liq.)"] = tabela["Dias_Zerar_20"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
        )
        tabela["Dias p/ Zerar (30% Liq.)"] = tabela["Dias_Zerar_30"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
        )
        cols.extend(["Dias p/ Zerar (20% Liq.)", "Dias p/ Zerar (30% Liq.)"])
    st.dataframe(tabela[cols], use_container_width=True, hide_index=True)

    st.markdown("---")

    # ---------- gráficos (iguais ao original) ----------
    st.subheader("Análise Visual da Carteira Consolidada", divider="blue")
    col_bar, col_line = st.columns(2)
    with col_bar:
        fig_bar = px.bar(
            tabela.head(999),
            x="Perc_PL", y="Ativo", orientation="h",
            title="Posições por % do PL", text_auto=".2f"
        )
        fig_bar.update_layout(
            yaxis={"categoryorder": "total ascending"},
            height=1000, xaxis_title="% do PL", yaxis_title="Ativo",
            uniformtext_minsize=8, uniformtext_mode="hide"
        )
        fig_bar.update_traces(textangle=0, textposition="outside")
        st.plotly_chart(fig_bar, use_container_width=True)

    with col_line:
        df_sorted = tabela.sort_values("Perc_PL", ascending=False).reset_index()
        df_sorted["CUM_PERC_PL"] = df_sorted["Perc_PL"].cumsum()
        df_sorted["POSICAO_RANK"] = df_sorted.index + 1
        fig_line = px.line(
            df_sorted, x="POSICAO_RANK", y="CUM_PERC_PL",
            title="Curva de Concentração da Carteira",
            markers=True, hover_name="Ativo"
        )
        fig_line.update_layout(
            xaxis_title="Ranking das Posições",
            yaxis_title="% Acumulado do PL",
            yaxis_ticksuffix="%"
        )
        # anotações top‑5 / top‑10
        if len(df_sorted) >= 5:
            y5 = df_sorted.loc[4, "CUM_PERC_PL"]
            fig_line.add_annotation(x=5, y=y5,
                text=f"<b>Top 5:</b><br>{y5:.1f}%", showarrow=True, arrowhead=2,
                ax=-40, ay=-40)
        if len(df_sorted) >= 10:
            y10 = df_sorted.loc[9, "CUM_PERC_PL"]
            fig_line.add_annotation(x=10, y=y10,
                text=f"<b>Top 10:</b><br>{y10:.1f}%", showarrow=True, arrowhead=2,
                ax=40, ay=-40)
        st.plotly_chart(fig_line, use_container_width=True)

    st.markdown("---")

    # evolução mensal — posições (% PL)
    st.header(f"Evolução Mensal da Carteira – {gestora_sel}", divider="blue")
    dados_evo = dados_gestora.copy()
    dados_evo["Perc_PL"] = (
        dados_evo["Valor_Consolidado_R"] / dados_evo["PL_Total_Gestora_Mes"] * 100
    )
    meses_ordem = sorted(dados_evo["DT_COMPTC"].unique())
    meses_fmt   = [pd.to_datetime(d).strftime("%b/%y") for d in meses_ordem]

    st.subheader("Posições em Ações (% do PL)", divider="blue")
    pivot_pl = dados_evo.pivot_table(index="Ativo", columns="DT_COMPTC",
                                     values="Perc_PL").fillna(0)
    df_pl = pivot_pl.reset_index().melt(id_vars="Ativo", var_name="Data",
                                        value_name="% do PL")
    df_pl["Mês"] = pd.to_datetime(df_pl["Data"]).dt.strftime("%b/%y")

    fig_evo_pl = go.Figure()
    for i, mes in enumerate(reversed(meses_fmt)):
        df_mes = df_pl[df_pl["Mês"] == mes]
        cor_idx = len(meses_fmt) - 1 - i
        fig_evo_pl.add_bar(
            y=df_mes["Ativo"], x=df_mes["% do PL"], name=mes, orientation="h",
            text=df_mes["% do PL"].apply(lambda x: f"{x:.2f}%"),
            marker_color=PALETA_DE_CORES[cor_idx % len(PALETA_DE_CORES)]
        )
    fig_evo_pl.update_layout(
        barmode="group", title="Comparativo Mensal de Posições (% do PL)",
        height=max(400, len(pivot_pl.index) * 35 * len(MESES_PARA_ANALISE)),
        yaxis_title="Ativo", xaxis_title="% do PL Consolidado",
        yaxis={"categoryorder": "total ascending"}, legend_title_text="Mês",
        legend=dict(traceorder="reversed")
    )
    st.plotly_chart(fig_evo_pl, use_container_width=True)

    # evolução mensal — participação na companhia
    st.subheader("Participação nas Companhias (% da Cia)", divider="blue")
    evo_cia = dados_evo.dropna(subset=["Perc_Cia"])
    if not evo_cia.empty:
        pivot_cia = evo_cia.pivot_table(index="Ativo", columns="DT_COMPTC",
                                        values="Perc_Cia").fillna(0)
        df_cia = pivot_cia.reset_index().melt(id_vars="Ativo", var_name="Data",
                                              value_name="% da Cia")
        df_cia["Mês"] = pd.to_datetime(df_cia["Data"]).dt.strftime("%b/%y")

        fig_evo_cia = go.Figure()
        for i, mes in enumerate(reversed(meses_fmt)):
            df_mes = df_cia[df_cia["Mês"] == mes]
            cor_idx = len(meses_fmt) - 1 - i
            fig_evo_cia.add_bar(
                y=df_mes["Ativo"], x=df_mes["% da Cia"], name=mes, orientation="h",
                text=df_mes["% da Cia"].apply(lambda x: f"{x:.2f}%"),
                marker_color=PALETA_DE_CORES[cor_idx % len(PALETA_DE_CORES)]
            )
        fig_evo_cia.update_layout(
            barmode="group", title="Comparativo Mensal de Participação (% da Companhia)",
            height=max(400, len(pivot_cia.index) * 35 * len(MESES_PARA_ANALISE)),
            yaxis_title="Ativo", xaxis_title="% da Companhia",
            yaxis={"categoryorder": "total ascending"}, legend_title_text="Mês",
            legend=dict(traceorder="reversed")
        )
        st.plotly_chart(fig_evo_cia, use_container_width=True)



# ==========================================================================
# PARTE 5: ABA 2 - ANÁLISE POR ATIVO
# ==========================================================================

# =======================================================================
# PÁGINA 2 – ANÁLISE POR ATIVO
# =======================================================================
elif pagina == "Análise por ativo":

    # --------- FILTRO EXCLUSIVO (sidebar) ---------
    with sidebar.container():
        st.header("Filtro por Ativo")
        lista_ativos = sorted(df_final["Ativo"].dropna().unique())
        if not lista_ativos:
            st.warning("Nenhum ativo encontrado nos dados processados.")
            st.stop()
        default_idx = lista_ativos.index("PETR4") if "PETR4" in lista_ativos else 0
        ativo_sel = st.selectbox(
            "Selecione o Ativo:",
            lista_ativos,
            index=default_idx,
            key="f_ativo",
        )

    # --------- CONTEÚDO PRINCIPAL ----------
    st.header("Análise por Ativo", divider="blue")

    datas_disp = sorted(df_final["DT_COMPTC"].unique(), reverse=True)
    if not datas_disp:
        st.warning("Nenhuma data disponível.")
        st.stop()

    mes_rec = datas_disp[0]
    df_ativo = df_final[
        (df_final["Ativo"] == ativo_sel) &
        (df_final["DT_COMPTC"] == mes_rec)
    ].copy()

    if df_ativo.empty:
        st.warning(f"Nenhum dado encontrado para o ativo {ativo_sel} no mês selecionado.")
        st.stop()

    st.subheader(f"Investidores em {ativo_sel} – {pd.to_datetime(mes_rec).strftime('%B de %Y')}",
                 divider="blue")

    # liquidez opcional
    if "Volume_Medio_Financeiro_60d" in df_ativo.columns:
        df_ativo["Dias_Zerar_20"] = df_ativo["Valor_Consolidado_R"] / (
            0.20 * df_ativo["Volume_Medio_Financeiro_60d"]
        ) / 1000
        df_ativo["Dias_Zerar_30"] = df_ativo["Valor_Consolidado_R"] / (
            0.30 * df_ativo["Volume_Medio_Financeiro_60d"]
        ) / 1000

    # tabela de gestoras
    df_display = df_ativo.sort_values("Valor_Consolidado_R", ascending=False).copy()
    df_display["Posição (R$)"] = df_display["Valor_Consolidado_R"].apply(formatar_moeda_brl)
    df_display["% da Cia"] = df_display["Perc_Cia"].apply(
        lambda x: f"{x:.2f}%" if pd.notna(x) else "N/A"
    )

    cols = ["Gestora", "Posição (R$)", "% da Cia"]
    if "Dias_Zerar_20" in df_display.columns:
        df_display["Dias p/ Zerar (20% Liq.)"] = df_display["Dias_Zerar_20"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
        )
        df_display["Dias p/ Zerar (30% Liq.)"] = df_display["Dias_Zerar_30"].apply(
            lambda x: f"{x:.1f}" if pd.notna(x) else "N/A"
        )
        cols.extend(["Dias p/ Zerar (20% Liq.)", "Dias p/ Zerar (30% Liq.)"])

    st.dataframe(df_display[cols], use_container_width=True, hide_index=True)
    st.markdown("---")

    # donut de participação
    total_perc = df_ativo["Perc_Cia"].sum()
    if pd.notna(total_perc) and total_perc < 100:
        df_donut = df_ativo[["Gestora", "Perc_Cia"]].copy()
        outros = 100 - total_perc
        df_donut = pd.concat(
            [df_donut, pd.DataFrame([{"Gestora": "Outros Acionistas", "Perc_Cia": outros}])]
        )
        fig_donut = px.pie(
            df_donut, names="Gestora", values="Perc_Cia",
            title=f"Distribuição da Propriedade de {ativo_sel}", hole=0.4
        )
        fig_donut.update_traces(textinfo="percent+label", textposition="inside")
        st.plotly_chart(fig_donut, use_container_width=True)


# ==========================================================================
# PARTE 6: ABA 3 - MOVIMENTAÇÕES RELEVANTES
# ==========================================================================

elif pagina == "Movimentações relevantes":

    # --------- FILTROS (sidebar) ---------
    with sidebar.container():
        st.header("Filtros de Movimentação")
        part_final_min = st.number_input(
            "Participação Final Mínima (% da Cia)",
            min_value=0.0, max_value=10.0, value=0.05, step=0.05, format="%.2f"
        )
        aumento_rel_min = st.number_input(
            "Aumento Relativo Mínimo (%)",
            min_value=10, max_value=1000, value=50, step=10
        )
        reducao_rel_min = st.number_input(
            "Redução Relativa Mínima (%)",
            min_value=10, max_value=100, value=25, step=5
        )

    # --------- CONTEÚDO PRINCIPAL ---------
    st.header("Análise de Movimentações Relevantes", divider="blue")

    # precisa de pelo menos 2 datas
    datas_disp = sorted(df_final["DT_COMPTC"].unique())
    if len(datas_disp) < 2:
        st.info("A análise requer pelo menos dois meses de dados.")
        st.stop()

    data_ini, data_fim = datas_disp[0], datas_disp[-1]
    base_cols = ["Gestora", "Ativo", "Perc_Cia",
                 "Valor_Consolidado_R", "PL_Total_Gestora_Mes"]

    df_ini = df_final[df_final["DT_COMPTC"] == data_ini][base_cols]
    df_fim = df_final[df_final["DT_COMPTC"] == data_fim][base_cols]

    mov = df_ini.merge(
        df_fim,
        on=["Gestora", "Ativo"],
        how="outer",
        suffixes=("_ini", "_fim")
    ).fillna(0)

    # ---------------- POSIÇÕES NOVAS ----------------
    st.subheader("Posições Novas", divider="blue")
    novas = mov[
        (mov["Perc_Cia_ini"] == 0) &
        (mov["Perc_Cia_fim"] > part_final_min)
    ].copy()

    if novas.empty:
        st.write("Nenhuma posição nova com os filtros selecionados.")
    else:
        novas["% PL Final"] = (
            novas["Valor_Consolidado_R_fim"] /
            novas["PL_Total_Gestora_Mes_fim"]
        ).replace([np.inf, -np.inf], 0) * 100
        novas["Part. Final"] = novas["Perc_Cia_fim"].apply(lambda x: f"{x:.3f}%")
        novas["% PL Final"]  = novas["% PL Final"].apply(lambda x: f"{x:.2f}%")
        st.dataframe(
            novas[["Gestora", "Ativo", "Part. Final", "% PL Final"]],
            use_container_width=True, hide_index=True
        )

    # ---------------- AUMENTOS ----------------
    st.subheader("Aumentos de Posição", divider="blue")
    aumentos = mov[
        (mov["Perc_Cia_ini"] > 0) &
        (mov["Perc_Cia_fim"] > mov["Perc_Cia_ini"])
    ].copy()

    if not aumentos.empty:
        aumentos["Var_Rel"] = (
            (aumentos["Perc_Cia_fim"] - aumentos["Perc_Cia_ini"]) /
            aumentos["Perc_Cia_ini"]
        ) * 100
        aumentos = aumentos[
            (aumentos["Var_Rel"] >= aumento_rel_min) &
            (aumentos["Perc_Cia_fim"] > part_final_min)
        ]

    if aumentos.empty:
        st.write("Nenhum aumento relevante.")
    else:
        aumentos["Part. Ini"]  = aumentos["Perc_Cia_ini"].apply(lambda x: f"{x:.3f}%")
        aumentos["Part. Fin"]  = aumentos["Perc_Cia_fim"].apply(lambda x: f"{x:.3f}%")
        aumentos["Var_Rel"]    = aumentos["Var_Rel"].apply(lambda x: f"{x:.0f}%")
        aumentos["% PL Final"] = (
            aumentos["Valor_Consolidado_R_fim"] /
            aumentos["PL_Total_Gestora_Mes_fim"]
        ).replace([np.inf, -np.inf], 0) * 100
        aumentos["% PL Final"] = aumentos["% PL Final"].apply(lambda x: f"{x:.2f}%")
        st.dataframe(
            aumentos[["Gestora", "Ativo", "Part. Ini", "Part. Fin",
                      "% PL Final", "Var_Rel"]],
            use_container_width=True, hide_index=True
        )

    # ---------------- REDUÇÕES ----------------
    st.subheader("Reduções de Posição", divider="blue")
    reducoes = mov[
        (mov["Perc_Cia_ini"] > 0) &
        (mov["Perc_Cia_fim"] < mov["Perc_Cia_ini"])
    ].copy()

    if not reducoes.empty:
        reducoes["Var_Rel"] = (
            (reducoes["Perc_Cia_ini"] - reducoes["Perc_Cia_fim"]) /
            reducoes["Perc_Cia_ini"]
        ) * 100
        reducoes = reducoes[reducoes["Var_Rel"] >= reducao_rel_min]

    if reducoes.empty:
        st.write("Nenhuma redução relevante.")
    else:
        reducoes["Part. Ini"] = reducoes["Perc_Cia_ini"].apply(lambda x: f"{x:.3f}%")
        reducoes["Part. Fin"] = reducoes["Perc_Cia_fim"].apply(lambda x: f"{x:.3f}%")
        reducoes["Var_Rel"]   = reducoes["Var_Rel"].apply(lambda x: f"{x:.0f}%")
        reducoes["% PL Final"] = (
            reducoes["Valor_Consolidado_R_fim"] /
            reducoes["PL_Total_Gestora_Mes_fim"]
        ).replace([np.inf, -np.inf], 0) * 100
        # se posição zerou, mostra "Zerou"
        reducoes["% PL Final"] = reducoes.apply(
            lambda r: "Zerou" if r["Perc_Cia_fim"] == 0
            else f"{r['% PL Final']:.2f}%", axis=1
        )
        st.dataframe(
            reducoes[["Gestora", "Ativo", "Part. Ini", "Part. Fin",
                      "% PL Final", "Var_Rel"]],
            use_container_width=True, hide_index=True
        )

# ==========================================================================
# PARTE 7: ABA 4 - RAZÃO DE TICKERS
# ==========================================================================

elif pagina == "Razão tickers":

    # --------- FILTROS (sidebar) ---------
    with sidebar.container():
        st.header("Parâmetros da Razão")

        # campos de ticker
        ticker1 = st.text_input("Ticker 1", "ITUB3").strip().upper()
        ticker2 = st.text_input("Ticker 2", "ITUB4").strip().upper()

        # inicializa datas na session_state
        today = date.today()
        if "start_date" not in st.session_state:
            st.session_state.start_date = date(2020, 1, 1)
        if "end_date" not in st.session_state:
            st.session_state.end_date = today

        # botões de período rápido
        st.subheader("Períodos rápidos")
        periodos = {
            "YTD": (date(today.year, 1, 1), today),
            "1M":  (today - relativedelta(months=1), today),
            "3M":  (today - relativedelta(months=3), today),
            "6M":  (today - relativedelta(months=6), today),
            "1Y":  (today - relativedelta(years=1), today),
            "2Y":  (today - relativedelta(years=2), today),
            "5Y":  (today - relativedelta(years=5), today),
            "10Y": (today - relativedelta(years=10), today),
        }
        cols1 = st.columns(4)
        for col, key in zip(cols1, ["YTD", "1M", "3M", "6M"]):
            if col.button(key):
                st.session_state.start_date, st.session_state.end_date = periodos[key]
        cols2 = st.columns(4)
        for col, key in zip(cols2, ["1Y", "2Y", "5Y", "10Y"]):
            if col.button(key):
                st.session_state.start_date, st.session_state.end_date = periodos[key]

        # datas manuais
        start_date = st.date_input(
            "Data Início", value=st.session_state.start_date, key="dt_ini"
        )
        end_date = st.date_input(
            "Data Fim", value=st.session_state.end_date, key="dt_fim"
        )

        # botão de atualização
        atualizar = st.button("Plotar Razão", use_container_width=True)

    # --------- CONTEÚDO PRINCIPAL ----------
    st.header("Razão de Tickers", divider="blue")

    # plot inicial ou após clique
    if atualizar:
        plotar_ratio(ticker1, ticker2, start_date, end_date)
    else:
        plotar_ratio(ticker1, ticker2,
                     st.session_state.start_date, st.session_state.end_date)


# ==========================================================================
# PARTE 8: ABA 5 - OVERLAP ENTRE GESTORAS
# ==========================================================================

elif pagina == "Overlap entre gestoras":

    with sidebar.container():
        st.header("Filtros de Overlap")
        datas_disp_ov = sorted(df_final["DT_COMPTC"].unique(), reverse=True)
        mes_ov = st.selectbox(
            "Mês de referência:",
            datas_disp_ov,
            format_func=lambda d: pd.to_datetime(d).strftime("%B de %Y"),
            key="f_mes_overlap"
        )
        min_peso = st.slider(
            "Peso mínimo no PL da gestora para contar como posição (%)",
            min_value=0.0, max_value=5.0, value=0.5, step=0.1,
            key="f_min_peso_overlap"
        )

    st.header("Overlap entre Gestoras", divider="blue")

    df_ov = df_final[df_final["DT_COMPTC"] == mes_ov].copy()
    df_ov["Perc_PL"] = df_ov["Valor_Consolidado_R"] / df_ov["PL_Total_Gestora_Mes"] * 100

    # Filtra posições acima do peso mínimo
    df_ov = df_ov[df_ov["Perc_PL"] >= min_peso]

    gestoras_ov = sorted(df_ov["Gestora"].unique())

    if len(gestoras_ov) < 2:
        st.warning("São necessárias pelo menos 2 gestoras com posições para calcular overlap.")
        st.stop()

    # Monta dicionário: gestora -> set de ativos
    carteiras = {g: set(df_ov[df_ov["Gestora"] == g]["Ativo"]) for g in gestoras_ov}

    # Matriz de overlap: % de ativos em comum em relação à união (Jaccard)
    n = len(gestoras_ov)
    matriz = np.zeros((n, n))
    for i, g1 in enumerate(gestoras_ov):
        for j, g2 in enumerate(gestoras_ov):
            if i == j:
                matriz[i][j] = 100.0
            else:
                intersec = carteiras[g1] & carteiras[g2]
                uniao    = carteiras[g1] | carteiras[g2]
                matriz[i][j] = (len(intersec) / len(uniao) * 100) if uniao else 0.0

    df_matriz = pd.DataFrame(matriz, index=gestoras_ov, columns=gestoras_ov)

    # --- Heatmap ---
    st.subheader(
        f"Heatmap de Similaridade – {pd.to_datetime(mes_ov).strftime('%B de %Y')}",
        divider="blue"
    )
    st.caption(
        "Índice de Jaccard: % de ativos em comum sobre a união das carteiras de cada par de gestoras. "
        f"Considera apenas posições com peso ≥ {min_peso:.1f}% do PL."
    )

    # Máscara: diagonal recebe NaN para não puxar a escala de cores.
    # A diagonal é plotada separadamente como anotação neutra.
    matriz_masked = matriz.astype(float).copy()
    np.fill_diagonal(matriz_masked, np.nan)

    # zmax dinâmico: maior valor fora da diagonal (mínimo 1 para não crashar)
    off_diag = matriz_masked[~np.isnan(matriz_masked)]
    zmax_real = float(off_diag.max()) if len(off_diag) > 0 else 1.0

    # Anotações: off-diagonal mostram o valor; diagonal mostra "■" em cinza
    anotacoes = [
        [("■" if i == j else f"{matriz[i][j]:.0f}%") for j in range(n)]
        for i in range(n)
    ]

    fig_hm = go.Figure(go.Heatmap(
        z=matriz_masked,
        x=gestoras_ov,
        y=gestoras_ov,
        text=anotacoes,
        texttemplate="%{text}",
        colorscale="Blues",
        zmin=0,
        zmax=zmax_real,
        colorbar=dict(title="Similaridade (%)"),
        hoverongaps=False,
        hovertemplate="<b>%{y}</b> × <b>%{x}</b><br>Overlap: %{z:.1f}%<extra></extra>",
        # Células NaN (diagonal) ficam com cor de fundo neutra
        showscale=True,
    ))

    # Sobrepõe anotações da diagonal em cinza médio para não sumir no branco
    for i, g in enumerate(gestoras_ov):
        fig_hm.add_annotation(
            x=g, y=g,
            text="100%",
            showarrow=False,
            font=dict(color="#666666", size=12),
            xref="x", yref="y",
        )

    fig_hm.update_layout(
        height=max(400, n * 60),
        xaxis=dict(tickangle=-35),
        margin=dict(l=20, r=20, t=40, b=40),
    )
    st.plotly_chart(fig_hm, use_container_width=True)

    st.markdown("---")

    # --- Detalhamento: ativos em comum entre par selecionado ---
    st.subheader("Detalhamento por Par de Gestoras", divider="blue")
    col_g1, col_g2 = st.columns(2)
    g1_sel = col_g1.selectbox("Gestora A:", gestoras_ov, key="ov_g1")
    g2_sel = col_g2.selectbox("Gestora B:", gestoras_ov,
                               index=min(1, len(gestoras_ov) - 1), key="ov_g2")

    if g1_sel == g2_sel:
        st.info("Selecione duas gestoras diferentes para ver os ativos em comum.")
    else:
        ativos_comuns = sorted(carteiras[g1_sel] & carteiras[g2_sel])
        if not ativos_comuns:
            st.write(f"Nenhum ativo em comum entre **{g1_sel}** e **{g2_sel}** com os filtros atuais.")
        else:
            st.markdown(
                f"**{len(ativos_comuns)} ativo(s) em comum** entre {g1_sel} e {g2_sel}:"
            )
            rows = []
            for ativo in ativos_comuns:
                r1 = df_ov[(df_ov["Gestora"] == g1_sel) & (df_ov["Ativo"] == ativo)]
                r2 = df_ov[(df_ov["Gestora"] == g2_sel) & (df_ov["Ativo"] == ativo)]
                val1  = r1["Valor_Consolidado_R"].values[0] if not r1.empty else np.nan
                val2  = r2["Valor_Consolidado_R"].values[0] if not r2.empty else np.nan
                perc1 = r1["Perc_PL"].values[0] if not r1.empty else np.nan
                perc2 = r2["Perc_PL"].values[0] if not r2.empty else np.nan
                rows.append({
                    "Ativo":                  ativo,
                    f"Valor {g1_sel} (R$)":   formatar_moeda_brl(val1),
                    f"% PL {g1_sel}":         f"{perc1:.2f}%" if pd.notna(perc1) else "N/A",
                    f"Valor {g2_sel} (R$)":   formatar_moeda_brl(val2),
                    f"% PL {g2_sel}":         f"{perc2:.2f}%" if pd.notna(perc2) else "N/A",
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


# ==========================================================================
# PARTE 9: ABA 6 - PRESSÃO DE LIQUIDEZ AGREGADA
# ==========================================================================

elif pagina == "Pressão de liquidez":

    # Thresholds do semáforo (dias p/ zerar agregado)
    LIMITE_VERDE    = 5    # até 5 dias: ok
    LIMITE_AMARELO  = 15   # 5–15 dias: atenção
                           # acima de 15: crítico

    with sidebar.container():
        st.header("Filtros de Liquidez")
        datas_disp_liq = sorted(df_final["DT_COMPTC"].unique(), reverse=True)
        mes_liq = st.selectbox(
            "Mês de referência:",
            datas_disp_liq,
            format_func=lambda d: pd.to_datetime(d).strftime("%B de %Y"),
            key="f_mes_liq"
        )
        st.markdown("---")
        st.markdown("**Thresholds do semáforo (dias p/ zerar)**")
        lim_verde   = st.number_input("🟢 Verde abaixo de:", value=LIMITE_VERDE,   min_value=1, key="lim_v")
        lim_amarelo = st.number_input("🟡 Amarelo abaixo de:", value=LIMITE_AMARELO, min_value=2, key="lim_a")

    st.header("Pressão de Liquidez Agregada", divider="blue")
    st.caption(
        "Pressão = soma dos dias necessários para todas as gestoras zerarem suas posições em cada ativo, "
        "assumindo que cada gestora pode negociar até 20% do volume médio diário de 60 dias."
    )

    # --- Prepara dados do mês selecionado ---
    df_liq = df_final[df_final["DT_COMPTC"] == mes_liq].copy()

    if "Volume_Medio_Financeiro_60d" not in df_liq.columns or df_liq["Volume_Medio_Financeiro_60d"].isna().all():
        st.warning("Dados de liquidez não disponíveis para o mês selecionado.")
        st.stop()

    liq_ok = df_liq["Volume_Medio_Financeiro_60d"].notna() & (df_liq["Volume_Medio_Financeiro_60d"] > 0)
    df_liq.loc[liq_ok, "Dias_Zerar_20"] = (
        df_liq.loc[liq_ok, "Valor_Consolidado_R"] /
        (0.20 * df_liq.loc[liq_ok, "Volume_Medio_Financeiro_60d"])
    ) / 1000

    # Agrega por ativo: soma dos dias de todas as gestoras
    pressao = (
        df_liq.groupby("Ativo")
        .agg(
            Pressao_Dias   =("Dias_Zerar_20",         "sum"),
            N_Gestoras     =("Gestora",               "nunique"),
            Valor_Total_R  =("Valor_Consolidado_R",   "sum"),
            Liq_Media_R    =("Volume_Medio_Financeiro_60d", "first"),
        )
        .reset_index()
        .dropna(subset=["Pressao_Dias"])
        .sort_values("Pressao_Dias", ascending=False)
    )

    def semaforo(dias):
        if dias < lim_verde:
            return "🟢"
        elif dias < lim_amarelo:
            return "🟡"
        else:
            return "🔴"

    pressao["Risco"] = pressao["Pressao_Dias"].apply(semaforo)

    # ---- SEÇÃO 1: Semáforo + Ranking ----
    st.subheader(
        f"Ranking de Pressão – {pd.to_datetime(mes_liq).strftime('%B de %Y')}",
        divider="blue"
    )

    col_v, col_a, col_r = st.columns(3)
    col_v.metric("🟢 Ativos ok",      len(pressao[pressao["Risco"] == "🟢"]))
    col_a.metric("🟡 Atenção",         len(pressao[pressao["Risco"] == "🟡"]))
    col_r.metric("🔴 Crítico",          len(pressao[pressao["Risco"] == "🔴"]))

    tabela_pressao = pressao.copy()
    tabela_pressao["Valor Total (R$)"]   = tabela_pressao["Valor_Total_R"].apply(formatar_moeda_brl)
    tabela_pressao["Pressão (dias)"]     = tabela_pressao["Pressao_Dias"].apply(lambda x: f"{x:.1f}")
    tabela_pressao["Gestoras expostas"]  = tabela_pressao["N_Gestoras"].astype(int)
    tabela_pressao["Liq. Média (R$ mil)"] = tabela_pressao["Liq_Media_R"].apply(
        lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A"
    )

    st.dataframe(
        tabela_pressao[["Risco", "Ativo", "Pressão (dias)", "Gestoras expostas",
                         "Valor Total (R$)", "Liq. Média (R$ mil)"]],
        use_container_width=True, hide_index=True
    )

    # ---- SEÇÃO 2: Gráfico de barras top-20 ----
    st.markdown("---")
    st.subheader("Top 20 – Ativos com Maior Pressão", divider="blue")

    top20 = pressao.head(20).sort_values("Pressao_Dias")
    cores_bar = top20["Risco"].map({"🟢": "#2ecc71", "🟡": "#f39c12", "🔴": "#e74c3c"})

    fig_bar_liq = go.Figure(go.Bar(
        x=top20["Pressao_Dias"],
        y=top20["Ativo"],
        orientation="h",
        marker_color=cores_bar.tolist(),
        text=top20["Pressao_Dias"].apply(lambda x: f"{x:.1f} dias"),
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Pressão: %{x:.1f} dias<br>"
            "<extra></extra>"
        )
    ))
    fig_bar_liq.add_vline(x=lim_verde,   line_dash="dot", line_color="#2ecc71",
                          annotation_text="Verde", annotation_position="top right")
    fig_bar_liq.add_vline(x=lim_amarelo, line_dash="dot", line_color="#f39c12",
                          annotation_text="Amarelo", annotation_position="top right")
    fig_bar_liq.update_layout(
        height=max(400, len(top20) * 32),
        xaxis_title="Dias p/ zerar (soma das gestoras, 20% liq.)",
        yaxis_title="Ativo",
        margin=dict(l=10, r=60, t=30, b=40),
    )
    st.plotly_chart(fig_bar_liq, use_container_width=True)

    # ---- SEÇÃO 3: Evolução mensal da pressão ----
    st.markdown("---")
    st.subheader("Evolução Mensal da Pressão por Ativo", divider="blue")

    # Seleciona ativos para acompanhar (default: top-5 do mês selecionado)
    top5_default = pressao.head(5)["Ativo"].tolist()
    todos_ativos_liq = sorted(df_final["Ativo"].dropna().unique())
    ativos_evo = st.multiselect(
        "Selecione ativos para acompanhar:",
        todos_ativos_liq,
        default=top5_default,
        key="evo_liq_ativos"
    )

    if not ativos_evo:
        st.info("Selecione ao menos um ativo acima.")
    else:
        # Calcula pressão por ativo/mês para todos os meses
        df_evo_liq = df_final[df_final["Ativo"].isin(ativos_evo)].copy()

        liq_ok_evo = (
            df_evo_liq["Volume_Medio_Financeiro_60d"].notna() &
            (df_evo_liq["Volume_Medio_Financeiro_60d"] > 0)
        )
        df_evo_liq.loc[liq_ok_evo, "Dias_Zerar_20"] = (
            df_evo_liq.loc[liq_ok_evo, "Valor_Consolidado_R"] /
            (0.20 * df_evo_liq.loc[liq_ok_evo, "Volume_Medio_Financeiro_60d"])
        ) / 1000

        pressao_evo = (
            df_evo_liq.groupby(["DT_COMPTC", "Ativo"])["Dias_Zerar_20"]
            .sum()
            .reset_index()
            .dropna()
        )
        pressao_evo["Mês"] = pd.to_datetime(pressao_evo["DT_COMPTC"]).dt.strftime("%b/%y")
        pressao_evo.sort_values("DT_COMPTC", inplace=True)

        fig_evo_liq = px.line(
            pressao_evo,
            x="Mês", y="Dias_Zerar_20", color="Ativo",
            markers=True,
            labels={"Dias_Zerar_20": "Pressão (dias)", "Mês": ""},
            title="Evolução da Pressão de Liquidez Agregada",
        )
        fig_evo_liq.add_hrect(
            y0=0, y1=lim_verde,
            fillcolor="#2ecc71", opacity=0.08, line_width=0,
            annotation_text="Verde", annotation_position="top left"
        )
        fig_evo_liq.add_hrect(
            y0=lim_verde, y1=lim_amarelo,
            fillcolor="#f39c12", opacity=0.08, line_width=0,
            annotation_text="Atenção", annotation_position="top left"
        )
        fig_evo_liq.add_hrect(
            y0=lim_amarelo, y1=fig_evo_liq.data[0].y.max() * 1.2 if fig_evo_liq.data else 100,
            fillcolor="#e74c3c", opacity=0.06, line_width=0,
            annotation_text="Crítico", annotation_position="top left"
        )
        fig_evo_liq.add_hline(y=lim_verde,   line_dash="dot", line_color="#2ecc71", line_width=1)
        fig_evo_liq.add_hline(y=lim_amarelo, line_dash="dot", line_color="#f39c12", line_width=1)
        fig_evo_liq.update_layout(
            height=500,
            yaxis_title="Dias p/ zerar (soma gestoras, 20% liq.)",
            legend_title_text="Ativo",
        )
        st.plotly_chart(fig_evo_liq, use_container_width=True)
