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

# --- CONFIGURAÇÕES DE LOCALIZAÇÃO E PÁGINA ---
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    st.error("Locale 'pt_BR.UTF-8' não encontrado. Usando a configuração padrão do sistema.")
    locale.setlocale(locale.LC_ALL, '')

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
MESES_PARA_ANALISE = ['202410', '202411', '202412']
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

if mapa_gestora_fundo is None or dados_brutos is None or df_market_caps is None or df_liquidez is None:
    st.error("Falha no carregamento de um dos arquivos base. O dashboard não pode continuar.");
    st.stop()

# --- Filtro com Tickers Limpos ---
try:
    with open(ARQUIVO_TICKERS_LIMPOS, 'r') as f:
        tickers_validos = json.load(f)
    #st.sidebar.success(f"{len(tickers_validos)} tickers válidos carregados.")
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

# ==========================================================================
# CÓDIGO FINAL E COMPLETO PARA A ABA 1
# ==========================================================================

tab1, tab2, tab3 = st.tabs(["Análise por Gestora", "Análise por Ativo", "Movimentações Relevantes"])

with tab1:
    st.header("Análise por Gestora", divider='blue')
    st.sidebar.divider()
    st.sidebar.header("Filtro por Gestora")

    lista_gestoras = sorted(df_final['Gestora'].unique())

    if not lista_gestoras:
        st.warning("Nenhuma gestora encontrada nos dados processados.")
    else:
        gestora_selecionada = st.sidebar.selectbox('Selecione a Gestora:', lista_gestoras, key='filtro_gestora')
        dados_gestora = df_final[df_final['Gestora'] == gestora_selecionada].copy()
        datas_disponiveis = sorted(dados_gestora['DT_COMPTC'].unique(), reverse=True)

        if not datas_disponiveis:
            st.warning(f"Nenhum dado de ações encontrado para a gestora {gestora_selecionada}.")
        else:
            mes_selecionado = datas_disponiveis[0]
            st.subheader(f"Visão Consolidada - {pd.to_datetime(mes_selecionado).strftime('%B de %Y')}",
                         divider='blue')
            dados_gestora_mes = dados_gestora[dados_gestora['DT_COMPTC'] == mes_selecionado].copy()

            pl_gestora_acoes = dados_gestora_mes['Valor_Consolidado_R'].sum()
            dados_gestora_mes['Perc_PL'] = (dados_gestora_mes[
                                                'Valor_Consolidado_R'] / pl_gestora_acoes) * 100 if pl_gestora_acoes > 0 else 0

            if 'Volume_Medio_Financeiro_60d' in dados_gestora_mes.columns:
                liquidez_valida = dados_gestora_mes['Volume_Medio_Financeiro_60d'].notna() & (
                        dados_gestora_mes['Volume_Medio_Financeiro_60d'] > 0)
                dados_gestora_mes.loc[liquidez_valida, 'Dias_Zerar_20'] = (dados_gestora_mes.loc[
                                                                               liquidez_valida, 'Valor_Consolidado_R'] / (
                                                                                   0.20 * dados_gestora_mes.loc[
                                                                               liquidez_valida, 'Volume_Medio_Financeiro_60d'])) / 1000
                dados_gestora_mes.loc[liquidez_valida, 'Dias_Zerar_30'] = (dados_gestora_mes.loc[
                                                                               liquidez_valida, 'Valor_Consolidado_R'] / (
                                                                                   0.30 * dados_gestora_mes.loc[
                                                                               liquidez_valida, 'Volume_Medio_Financeiro_60d'])) / 1000

            col1, col2 = st.columns(2)
            col1.metric("PL em Ações (Consolidado)", f"R$ {pl_gestora_acoes:,.2f}")
            col2.metric("Nº de Ativos na Carteira", f"{len(dados_gestora_mes)}")

            st.subheader("Exposição Total em Ações (Consolidado)")
            tabela_para_exibir = dados_gestora_mes.sort_values(by='Perc_PL', ascending=False)
            df_display = tabela_para_exibir.copy()
            df_display['Valor (R$)'] = df_display['Valor_Consolidado_R'].apply(
                lambda x: locale.currency(x, symbol=True, grouping=True))
            df_display['Market Cap (R$)'] = df_display['Market_Cap_Cia_R'].apply(
                lambda x: 'R$ ' + locale.format_string('%.0f', x, grouping=True) if pd.notna(x) else 'N/A')
            df_display['% do PL'] = df_display['Perc_PL'].apply(lambda x: f'{x:.2f}%')
            df_display['% da Cia'] = df_display['Perc_Cia'].apply(lambda x: f'{x:.2f}%' if pd.notna(x) else 'N/A')

            colunas_tabela = ['Ativo', 'Valor (R$)', '% do PL', 'Market Cap (R$)', '% da Cia']
            if 'Dias_Zerar_20' in df_display.columns:
                df_display['Dias p/ Zerar (20% Liq.)'] = df_display['Dias_Zerar_20'].apply(
                    lambda x: f'{x:.1f}' if pd.notna(x) else 'N/A')
                df_display['Dias p/ Zerar (30% Liq.)'] = df_display['Dias_Zerar_30'].apply(
                    lambda x: f'{x:.1f}' if pd.notna(x) else 'N/A')
                colunas_tabela.extend(['Dias p/ Zerar (20% Liq.)', 'Dias p/ Zerar (30% Liq.)'])

            st.dataframe(df_display[colunas_tabela], use_container_width=True, hide_index=True)
            st.markdown("---")

            st.subheader("Análise Visual da Carteira Consolidada", divider='blue')
            col_bar, col_line = st.columns(2)
            with col_bar:
                fig_bar = px.bar(tabela_para_exibir.head(999), x='Perc_PL', y='Ativo', orientation='h',
                                 title='Posições por % do PL', text_auto='.2f')
                fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'}, height=1000, xaxis_title="% do PL",
                                      yaxis_title="Ativo", uniformtext_minsize=8, uniformtext_mode='hide')
                fig_bar.update_traces(textangle=0, textposition='outside')
                st.plotly_chart(fig_bar, use_container_width=True)
            with col_line:
                df_sorted = tabela_para_exibir.sort_values(by='Perc_PL', ascending=False).reset_index()
                df_sorted['CUM_PERC_PL'] = df_sorted['Perc_PL'].cumsum()
                df_sorted['POSICAO_RANK'] = df_sorted.index + 1
                fig_line = px.line(df_sorted, x='POSICAO_RANK', y='CUM_PERC_PL',
                                   title='Curva de Concentração da Carteira', markers=True, hover_name='Ativo')
                fig_line.update_layout(xaxis_title="Ranking das Posições", yaxis_title="% Acumulado do PL",
                                       yaxis_ticksuffix="%")

                if len(df_sorted) >= 5:
                    y_top5 = df_sorted.loc[4, 'CUM_PERC_PL']
                    fig_line.add_annotation(x=5, y=y_top5,
                                            text=f"<b>Top 5:</b><br>{y_top5:.1f}%",
                                            showarrow=True, arrowhead=2,
                                            ax=-40, ay=-40)

                if len(df_sorted) >= 10:
                    y_top10 = df_sorted.loc[9, 'CUM_PERC_PL']
                    fig_line.add_annotation(x=10, y=y_top10,
                                            text=f"<b>Top 10:</b><br>{y_top10:.1f}%",
                                            showarrow=True, arrowhead=2,
                                            ax=40, ay=-40)

                st.plotly_chart(fig_line, use_container_width=True)

            st.markdown("---")

            st.header(f"Evolução Mensal da Carteira: {gestora_selecionada}", divider='blue')
            dados_gestora_evolucao = dados_gestora.copy()
            dados_gestora_evolucao['Perc_PL'] = (dados_gestora_evolucao['Valor_Consolidado_R'] / dados_gestora_evolucao[
                'PL_Total_Gestora_Mes']) * 100

            datas_ordenadas = sorted(dados_gestora_evolucao['DT_COMPTC'].unique())
            ordem_dos_meses = [pd.to_datetime(d).strftime('%b/%y') for d in datas_ordenadas]

            st.subheader("Posições em Ações (% do PL)", divider='blue')
            tabela_pivot_pl = dados_gestora_evolucao.pivot_table(index='Ativo', columns='DT_COMPTC',
                                                                 values='Perc_PL').fillna(0)
            df_plot_pl = tabela_pivot_pl.reset_index().melt(id_vars='Ativo', var_name='Data', value_name='% do PL')
            df_plot_pl['Mês'] = pd.to_datetime(df_plot_pl['Data']).dt.strftime('%b/%y')

            fig_evol_pl = go.Figure()
            for i, mes in enumerate(reversed(ordem_dos_meses)):
                df_mes_filtrado = df_plot_pl[df_plot_pl['Mês'] == mes]
                cor_index = len(ordem_dos_meses) - 1 - i
                fig_evol_pl.add_trace(go.Bar(
                    y=df_mes_filtrado['Ativo'], x=df_mes_filtrado['% do PL'], name=mes, orientation='h',
                    text=df_mes_filtrado['% do PL'].apply(lambda x: f'{x:.2f}%'), textposition='outside',
                    marker_color=PALETA_DE_CORES[cor_index % len(PALETA_DE_CORES)]
                ))
            fig_evol_pl.update_layout(
                barmode='group', title='Comparativo Mensal de Posições (% do PL)',
                height=max(400, len(tabela_pivot_pl.index) * 35 * len(MESES_PARA_ANALISE)),
                yaxis_title="Ativo", xaxis_title="% do PL Consolidado",
                yaxis={'categoryorder': 'total ascending'}, legend_title_text='Mês',
                legend=dict(traceorder='reversed')
            )
            st.plotly_chart(fig_evol_pl, use_container_width=True)

            st.subheader("Participação nas Companhias (% da Cia)", divider='blue')
            dados_evolucao_cia = dados_gestora_evolucao.dropna(subset=['Perc_Cia'])
            if not dados_evolucao_cia.empty:
                tabela_pivot_cia = dados_evolucao_cia.pivot_table(index='Ativo', columns='DT_COMPTC',
                                                                  values='Perc_Cia').fillna(0)
                df_plot_cia = tabela_pivot_cia.reset_index().melt(id_vars='Ativo', var_name='Data',
                                                                  value_name='% da Cia')
                df_plot_cia['Mês'] = pd.to_datetime(df_plot_cia['Data']).dt.strftime('%b/%y')

                fig_evol_cia = go.Figure()
                for i, mes in enumerate(reversed(ordem_dos_meses)):
                    df_mes_filtrado = df_plot_cia[df_plot_cia['Mês'] == mes]
                    cor_index = len(ordem_dos_meses) - 1 - i
                    fig_evol_cia.add_trace(go.Bar(
                        y=df_mes_filtrado['Ativo'], x=df_mes_filtrado['% da Cia'], name=mes, orientation='h',
                        text=df_mes_filtrado['% da Cia'].apply(lambda x: f'{x:.2f}%'), textposition='outside',
                        marker_color=PALETA_DE_CORES[cor_index % len(PALETA_DE_CORES)]
                    ))
                fig_evol_cia.update_layout(
                    barmode='group', title='Comparativo Mensal de Participação (% da Companhia)',
                    height=max(400, len(tabela_pivot_cia.index) * 35 * len(MESES_PARA_ANALISE)),
                    yaxis_title="Ativo", xaxis_title="% da Companhia",
                    yaxis={'categoryorder': 'total ascending'}, legend_title_text='Mês',
                    legend=dict(traceorder='reversed')
                )
                st.plotly_chart(fig_evol_cia, use_container_width=True)


# ==========================================================================
# PARTE 5: ABA 2 - ANÁLISE POR ATIVO
# ==========================================================================
with tab2:
    st.header("Análise por Ativo", divider='blue')
    st.sidebar.divider()
    st.sidebar.header("Filtro por Ativo")

    lista_ativos = sorted(df_final['Ativo'].dropna().unique())
    if lista_ativos:
        default_index = lista_ativos.index('PETR4') if 'PETR4' in lista_ativos else 0
        ativo_selecionado = st.sidebar.selectbox("Selecione o Ativo:", options=lista_ativos, index=default_index,
                                                 key='filtro_ativo')

        datas_disponiveis_geral = sorted(df_final['DT_COMPTC'].unique(), reverse=True)
        if datas_disponiveis_geral:
            mes_recente_geral = datas_disponiveis_geral[0]
            df_filtrado_ativo = df_final[
                (df_final['Ativo'] == ativo_selecionado) & (df_final['DT_COMPTC'] == mes_recente_geral)].copy()

            st.subheader(f"Investidores para o Ativo: {ativo_selecionado}", divider='blue')
            if not df_filtrado_ativo.empty:
                st.write(
                    f"**Posições das Gestoras da Lista em {pd.to_datetime(mes_recente_geral).strftime('%B de %Y')}**")

                if 'Volume_Medio_Financeiro_60d' in df_filtrado_ativo.columns:
                    df_filtrado_ativo.loc[:, 'Dias_Zerar_20'] = df_filtrado_ativo['Valor_Consolidado_R'] / (
                                0.20 * df_filtrado_ativo['Volume_Medio_Financeiro_60d']) / 1000
                    df_filtrado_ativo.loc[:, 'Dias_Zerar_30'] = df_filtrado_ativo['Valor_Consolidado_R'] / (
                                0.30 * df_filtrado_ativo['Volume_Medio_Financeiro_60d']) / 1000

                df_display_ativo = df_filtrado_ativo.sort_values(by="Valor_Consolidado_R", ascending=False)
                df_display_ativo['Posição (R$)'] = df_display_ativo['Valor_Consolidado_R'].apply(
                    lambda x: locale.currency(x, symbol=True, grouping=True))
                df_display_ativo['% da Cia'] = df_display_ativo['Perc_Cia'].apply(
                    lambda x: f'{x:.2f}%' if pd.notna(x) else 'N/A')

                colunas_tabela_ativo = ['Gestora', 'Posição (R$)', '% da Cia']
                if 'Dias_Zerar_20' in df_display_ativo.columns:
                    df_display_ativo['Dias p/ Zerar (20% Liq.)'] = df_display_ativo['Dias_Zerar_20'].apply(
                        lambda x: f'{x:.1f}' if pd.notna(x) else 'N/A')
                    df_display_ativo['Dias p/ Zerar (30% Liq.)'] = df_display_ativo['Dias_Zerar_30'].apply(
                        lambda x: f'{x:.1f}' if pd.notna(x) else 'N/A')
                    colunas_tabela_ativo.extend(['Dias p/ Zerar (20% Liq.)', 'Dias p/ Zerar (30% Liq.)'])

                st.dataframe(df_display_ativo[colunas_tabela_ativo], use_container_width=True, hide_index=True)
                st.markdown("---")

                total_perc_gestoras = df_filtrado_ativo['Perc_Cia'].sum()
                if pd.notna(total_perc_gestoras) and total_perc_gestoras < 100:
                    perc_outros = 100 - total_perc_gestoras
                    df_para_donut = df_filtrado_ativo[['Gestora', 'Perc_Cia']].copy()
                    outros_row = pd.DataFrame([{'Gestora': 'Outros Acionistas', 'Perc_Cia': perc_outros}])
                    df_para_donut = pd.concat([df_para_donut, outros_row], ignore_index=True)

                    st.write(f"**Contexto de Propriedade de {ativo_selecionado}**")
                    fig_rosca = px.pie(df_para_donut, names='Gestora', values='Perc_Cia',
                                       title=f"Distribuição da Participação em {ativo_selecionado}", hole=0.4)
                    fig_rosca.update_traces(textinfo='percent+label', textposition='inside')
                    st.plotly_chart(fig_rosca, use_container_width=True)
            else:
                st.warning(f"Nenhum dado encontrado para o ativo {ativo_selecionado} no mês mais recente.")
    else:
        st.warning("Nenhum ativo encontrado nos dados processados.")

# ==========================================================================
# PARTE 6: ABA 3 - MOVIMENTAÇÕES RELEVANTES
# ==========================================================================
with tab3:
    st.header("Análise de Movimentações Relevantes", divider='blue')

    datas_unicas = df_final['DT_COMPTC'].unique()
    if len(datas_unicas) < 2:
        st.warning("A análise de movimentação requer pelo menos dois meses de dados.")
    else:
        data_inicio = df_final['DT_COMPTC'].min()
        data_fim = df_final['DT_COMPTC'].max()

        st.sidebar.divider()
        st.sidebar.header("Filtros de Movimentação")
        part_final_min = st.sidebar.number_input('Participação Final Mínima (% da Cia)', 0.0, 10.0, 0.05, 0.05, "%.2f",
                                                 help="Para evitar ruídos, exibir apenas movimentos que resultaram em uma participação final maior que este valor.")
        aumento_relativo_min = st.sidebar.number_input('Aumento Relativo Mínimo (%)', min_value=10, max_value=1000,
                                                       value=50, step=10,
                                                       help="Sensibilidade para AUMENTOS. Apenas posições que aumentaram mais que X% serão exibidas.")
        reducao_relativa_min = st.sidebar.number_input('Redução Relativa Mínima (%)', min_value=10, max_value=100,
                                                       value=25, step=5,
                                                       help="Sensibilidade para REDUÇÕES. Apenas posições que reduziram mais que X% serão exibidas.")

        cols_para_analise = ['Gestora', 'Ativo', 'Perc_Cia', 'Valor_Consolidado_R', 'PL_Total_Gestora_Mes']
        df_inicio = df_final[df_final['DT_COMPTC'] == data_inicio][cols_para_analise]
        df_fim = df_final[df_final['DT_COMPTC'] == data_fim][cols_para_analise]

        movimentos_df = pd.merge(df_inicio, df_fim, on=['Gestora', 'Ativo'], how='outer',
                                 suffixes=('_inicio', '_fim')).fillna(0)

        st.subheader("Posições Novas",
                     help="Ativos que não existiam na carteira da gestora no primeiro mês, mas existem no último e superam o filtro de participação mínima.",  divider='blue')
        posicoes_novas_df = movimentos_df[
            (movimentos_df['Perc_Cia_inicio'] == 0) &
            (movimentos_df['Perc_Cia_fim'] > part_final_min)
            ].copy()

        if posicoes_novas_df.empty:
            st.info("Nenhuma posição nova encontrada com os filtros selecionados.")
        else:
            posicoes_novas_df['% PL Final'] = (posicoes_novas_df['Valor_Consolidado_R_fim'] / posicoes_novas_df[
                'PL_Total_Gestora_Mes_fim']).replace([np.inf, -np.inf], 0) * 100
            posicoes_novas_df['Part. Final (% da Cia)'] = posicoes_novas_df['Perc_Cia_fim'].apply(lambda x: f'{x:.3f}%')
            posicoes_novas_df['% PL Final'] = posicoes_novas_df['% PL Final'].apply(lambda x: f'{x:.2f}%')
            st.dataframe(
                posicoes_novas_df.sort_values(by='Valor_Consolidado_R_fim', ascending=False)[
                    ['Gestora', 'Ativo', 'Part. Final (% da Cia)', '% PL Final']],
                use_container_width=True, hide_index=True)

        st.subheader("Aumentos de Posição",
                     help="Posições existentes que aumentaram acima do filtro de Aumento Relativo e resultaram em uma posição final maior que o filtro de Participação Mínima.",  divider='blue')
        aumentos_df = movimentos_df[
            (movimentos_df['Perc_Cia_inicio'] > 0) &
            (movimentos_df['Perc_Cia_fim'] > movimentos_df['Perc_Cia_inicio'])
            ].copy()

        if not aumentos_df.empty:
            aumentos_df['Variacao_Relativa'] = ((aumentos_df['Perc_Cia_fim'] - aumentos_df['Perc_Cia_inicio']) /
                                                aumentos_df['Perc_Cia_inicio']) * 100
            aumentos_filtrados = aumentos_df[
                (aumentos_df['Variacao_Relativa'] >= aumento_relativo_min) &
                (aumentos_df['Perc_Cia_fim'] > part_final_min)
                ]
            if aumentos_filtrados.empty:
                st.info("Nenhum aumento de posição relevante encontrado com os filtros selecionados.")
            else:
                df_display_aumento = aumentos_filtrados.sort_values(by='Variacao_Relativa', ascending=False).copy()
                df_display_aumento['% PL Final'] = (df_display_aumento['Valor_Consolidado_R_fim'] / df_display_aumento[
                    'PL_Total_Gestora_Mes_fim']).replace([np.inf, -np.inf], 0) * 100
                df_display_aumento['Part. Inicial'] = df_display_aumento['Perc_Cia_inicio'].apply(lambda x: f'{x:.3f}%')
                df_display_aumento['Part. Final'] = df_display_aumento['Perc_Cia_fim'].apply(lambda x: f'{x:.3f}%')
                df_display_aumento['Aumento Relativo'] = df_display_aumento['Variacao_Relativa'].apply(
                    lambda x: f'{x:.0f}%')
                df_display_aumento['% PL Final'] = df_display_aumento['% PL Final'].apply(lambda x: f'{x:.2f}%')
                st.dataframe(df_display_aumento[['Gestora', 'Ativo', 'Part. Inicial', 'Part. Final', '% PL Final',
                                                 'Aumento Relativo']],
                             use_container_width=True, hide_index=True)

        st.subheader("Reduções de Posição",
                     help="Posições existentes que foram reduzidas acima do filtro de Redução Relativa.", divider='blue')
        reducoes_df = movimentos_df[
            (movimentos_df['Perc_Cia_fim'] < movimentos_df['Perc_Cia_inicio'])
        ].copy()

        if not reducoes_df.empty:
            reducoes_df['Variacao_Relativa'] = ((reducoes_df['Perc_Cia_inicio'] - reducoes_df['Perc_Cia_fim']) /
                                                reducoes_df['Perc_Cia_inicio']) * 100
            reducoes_filtradas = reducoes_df[reducoes_df['Variacao_Relativa'] >= reducao_relativa_min]
            if reducoes_filtradas.empty:
                st.info("Nenhuma redução de posição relevante encontrada com os filtros selecionados.")
            else:
                df_display_reducao = reducoes_filtradas.sort_values(by='Variacao_Relativa', ascending=False).copy()
                df_display_reducao['% PL Final'] = (df_display_reducao['Valor_Consolidado_R_fim'] / df_display_reducao[
                    'PL_Total_Gestora_Mes_fim']).replace([np.inf, -np.inf], 0) * 100
                df_display_reducao['Part. Inicial'] = df_display_reducao['Perc_Cia_inicio'].apply(lambda x: f'{x:.3f}%')
                df_display_reducao['Part. Final'] = df_display_reducao['Perc_Cia_fim'].apply(lambda x: f'{x:.3f}%')
                df_display_reducao['Redução Relativa'] = df_display_reducao['Variacao_Relativa'].apply(
                    lambda x: f'{x:.0f}%')

                formatted_pl = df_display_reducao['% PL Final'].apply(lambda x: f'{x:.2f}%')
                df_display_reducao['% PL Final Formatado'] = np.where(df_display_reducao['Perc_Cia_fim'] == 0, "Zerou",
                                                                      formatted_pl)

                st.dataframe(df_display_reducao[
                                 ['Gestora', 'Ativo', 'Part. Inicial', 'Part. Final', '% PL Final Formatado',
                                  'Redução Relativa']],
                             use_container_width=True, hide_index=True)
