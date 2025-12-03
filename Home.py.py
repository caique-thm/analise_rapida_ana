import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px

# Configuração da página
st.set_page_config(
    page_title="Análise Pluviométrica - ANA",
    layout="wide",
    page_icon="🌧️"
)

st.title("🌧️ Dashboard de Análise de Dados Pluviométricos")
st.markdown("Este aplicativo processa dados brutos, realiza amostragem e calcula métricas de completude e falhas.")

# --- BARRA LATERAL (Configurações) ---
st.sidebar.header("📂 Configuração de Dados")

# Opção de carregar arquivo ou usar caminho local
data_source = st.sidebar.radio("Fonte de Dados", ["Upload de Arquivo", "Caminho Local"])

df = None

if data_source == "Upload de Arquivo":
    uploaded_file = st.sidebar.file_uploader("Arraste seu CSV aqui", type=["csv"])
    if uploaded_file:
        try:
            df = pd.read_csv(uploaded_file)
        except Exception as e:
            st.error(f"Erro ao ler arquivo: {e}")
else:
    # Caminho atualizado conforme solicitado
    default_path = r"G:\Meu Drive\Dados_PLuviometrico_ANA\resultado\df_dados_tratados.csv"
    local_path = st.sidebar.text_input("Caminho do Arquivo", default_path)
    
    if st.sidebar.button("Carregar do Caminho"):
        try:
            df = pd.read_csv(local_path)
        except Exception as e:
            st.error(f"Erro ao ler do caminho especificado. Verifique se o caminho existe. Erro: {e}")

# Parâmetros da Amostragem
st.sidebar.markdown("---")
st.sidebar.header("⚙️ Parâmetros")
porcentagem_amostra = st.sidebar.slider("Porcentagem da Amostra (%)", 1, 100, 10, help="Define a fração de estações por estado.") / 100

# --- PROCESSAMENTO PRINCIPAL ---

if df is not None:
    st.success("Dados carregados com sucesso!")
    
    # --- TABELA 1: Visão Geral por Estado ---
    st.header("📊 1. Visão Geral das Estações (Tabela 1)")
    
    with st.expander("Ver código e detalhes da Tabela 1"):
        st.code("""
        df_estado = df.groupby('estado')['EstacaoCodigo'].nunique().to_frame()
        df_estado = df_estado.reset_index().rename(columns={'EstacaoCodigo': 'qtd_estacao'})
        df_estado['pct_estacao'] = round((df_estado['qtd_estacao']/sum(df_estado['qtd_estacao']))*100,2)
        """)

    df_estado = df.groupby('estado')['EstacaoCodigo'].nunique().to_frame()
    df_estado = df_estado.reset_index().rename(columns={'EstacaoCodigo': 'qtd_estacao'})
    df_estado['pct_estacao'] = round((df_estado['qtd_estacao']/sum(df_estado['qtd_estacao']))*100, 2)
    
    st.dataframe(df_estado, use_container_width=True)

    # --- TABELA 2: Processamento Pesado (Cacheado) ---
    st.header("🛠️ 2. Processamento e Cálculo de Métricas (Tabela 2)")
    st.info(f"Executando amostragem de {porcentagem_amostra*100}% e tratamento de datas. Isso pode levar um momento...")

    # Função cacheada para processamento pesado
    @st.cache_data(show_spinner=True)
    def processar_dados_complexos(df_input, sample_frac):
        # 1. AMOSTRAGEM ESTRATIFICADA
        estacoes_unicas = df_input[['estado', 'EstacaoCodigo']].drop_duplicates()
        estacoes_sorteadas = estacoes_unicas.groupby('estado').sample(frac=sample_frac, random_state=42)
        
        # Filtra os dados
        df_amostra = df_input[df_input['EstacaoCodigo'].isin(estacoes_sorteadas['EstacaoCodigo'])].copy()

        # BLINDAGEM 1: Remove duplicatas de MÊS
        linhas_antes = len(df_amostra)
        df_amostra = df_amostra.drop_duplicates(subset=['EstacaoCodigo', 'Ano', 'Mes'])
        msg_duplicatas_mes = f"Linhas removidas (duplicatas de mês): {linhas_antes - len(df_amostra)}"

        # 2. PREPARAÇÃO DOS DADOS (MELT)
        colunas_chuva = [f'Chuva{i:02d}' for i in range(1, 32)]
        
        # Verifica se as colunas existem antes do melt
        colunas_existentes = [c for c in colunas_chuva if c in df_amostra.columns]
        
        df_melted = df_amostra.melt(
            id_vars=['EstacaoCodigo', 'estado', 'Ano', 'Mes'], 
            value_vars=colunas_existentes,
            var_name='Dia_str',
            value_name='Valor'
        )

        # Limpeza básica
        df_melted = df_melted.dropna(subset=['Valor'])
        df_melted['Dia'] = df_melted['Dia_str'].str.replace('Chuva', '').astype(int)

        # Cria a Data Real
        df_melted['Data_Real'] = pd.to_datetime(
            dict(year=df_melted.Ano, month=df_melted.Mes, day=df_melted.Dia), 
            errors='coerce'
        )

        # Remove datas inválidas
        df_melted = df_melted.dropna(subset=['Data_Real'])

        # BLINDAGEM 2: Remove duplicatas de DIA
        len_antes_dia = len(df_melted)
        df_melted = df_melted.drop_duplicates(subset=['EstacaoCodigo', 'Data_Real'])
        msg_duplicatas_dia = f"Registros diários removidos (duplicatas reais): {len_antes_dia - len(df_melted)}"

        # Ordenação obrigatória para o .diff()
        df_melted = df_melted.sort_values(['EstacaoCodigo', 'Data_Real'])

        # 3. CÁLCULOS POR ANO
        grupos = df_melted.groupby(['EstacaoCodigo', 'Ano'])

        # Métrica A: Intervalos (Gaps)
        df_melted['gap'] = grupos['Data_Real'].diff().dt.days

        resumo_intervalos = grupos['gap'].agg(
            Intervalo_Max='max',
            Intervalo_Medio='mean',
            Intervalo_Min='min'
        ).fillna(0)

        # Métrica B: Completude
        contagem_dias = grupos.size().to_frame(name='dias_com_dados')

        # Junta tudo
        df_final = contagem_dias.join(resumo_intervalos).reset_index()

        # Recupera o estado
        # Otimização: Merge apenas com as estações sorteadas que já temos em memória
        df_final = df_final.merge(estacoes_sorteadas, on='EstacaoCodigo', how='left')

        # Cálculos finais
        def get_dias_ano(ano):
            return 366 if (ano % 4 == 0 and ano % 100 != 0) or (ano % 400 == 0) else 365

        df_final['total_dias_ano'] = df_final['Ano'].apply(get_dias_ano)
        df_final['Completude_%'] = (df_final['dias_com_dados'] / df_final['total_dias_ano']) * 100

        # Arredondamentos
        df_final['Completude_%'] = df_final['Completude_%'].round(2)
        df_final['Intervalo_Medio'] = df_final['Intervalo_Medio'].round(2)

        colunas_finais = ['EstacaoCodigo', 'estado', 'Ano', 'Completude_%', 'Intervalo_Max', 'Intervalo_Medio', 'dias_com_dados']
        return df_final[colunas_finais], msg_duplicatas_mes, msg_duplicatas_dia

    # Executa a função
    df_final, msg1, msg2 = processar_dados_complexos(df, porcentagem_amostra)

    # Exibe logs e tabela
    st.text(msg1)
    st.text(msg2)
    st.write("Amostra dos dados processados:")
    st.dataframe(df_final.head(50), use_container_width=True)

    st.download_button(
        label="📥 Baixar Tabela Processada (CSV)",
        data=df_final.to_csv(index=False).encode('utf-8'),
        file_name='dados_analisados.csv',
        mime='text/csv',
    )

    # --- TABELA 3: Estatísticas Descritivas ---
    st.header("📈 3. Estatísticas por Ano e Estado (Tabela 3)")
    
    # Filtro dinâmico de anos
    anos_disponiveis = sorted(df_final['Ano'].unique())
    anos_selecionados = st.multiselect(
        "Selecione os Anos para Análise", 
        options=anos_disponiveis, 
        default=[ano for ano in [2021, 2022, 2023, 2024] if ano in anos_disponiveis]
    )

    if anos_selecionados:
        df_filtrado = df_final[df_final['Ano'].isin(anos_selecionados)]

        # Agrupa e calcula
        resumo_completo = df_filtrado.groupby(['Ano', 'estado'])['Completude_%'].agg(['mean', 'median', 'std', 'count'])

        resumo_completo = resumo_completo.rename(columns={
            'mean': 'Média',
            'median': 'Mediana',
            'std': 'Desvio Padrão',
            'count': 'Qtd Estações'
        })
        
        st.dataframe(resumo_completo.round(2), use_container_width=True)
    else:
        st.warning("Selecione pelo menos um ano.")

    # --- TABELA 4 (GRÁFICO): Plotly ---
    st.header("📉 4. Evolução da Completude (Gráficos)")
    
    col_g1, col_g2 = st.columns(2)

    with col_g1:
        st.subheader("Por Estado")
        df_grouped_graph = df_final.groupby(['Ano', 'estado'])['Completude_%'].mean().reset_index()
        fig = px.line(
            df_grouped_graph, 
            x='Ano', 
            y='Completude_%', 
            color='estado', 
            markers=True,
            title='Média de Completude por Estado'
        )
        st.plotly_chart(fig, use_container_width=True)

    with col_g2:
        st.subheader("Visão Geral (Brasil)")
        # 1. Filtro e Agrupamento (Do seu código)
        df_grouped_geral = df_final.groupby(['Ano'])['Completude_%'].mean().reset_index()

        # 2. Gráfico
        fig_geral = px.line(
            df_grouped_geral, 
            x='Ano', 
            y='Completude_%',  
            markers=True,
            title='Média de Completude por Ano (Geral)'
        )
        st.plotly_chart(fig_geral, use_container_width=True)

    # --- 5. TESTE DE ESTABILIDADE (AUTOMÁTICO) ---
    st.markdown("---")
    st.header("🧪 5. Resultado do Teste de Estabilidade")
    st.markdown("O teste abaixo roda automaticamente simulando 5 amostragens diferentes para garantir que seus resultados são confiáveis.")

    @st.cache_data(show_spinner=True)
    def executar_teste_estabilidade(df_input, sample_frac):
        seeds_para_testar = [42, 1, 100, 2024, 999]
        resultados_temp = []
        
        def get_dias_ano_teste(ano):
            return 366 if (ano % 4 == 0 and ano % 100 != 0) or (ano % 400 == 0) else 365

        for seed in seeds_para_testar:
            # 1. AMOSTRAGEM
            estacoes_unicas = df_input[['estado', 'EstacaoCodigo']].drop_duplicates()
            estacoes_sorteadas = estacoes_unicas.groupby('estado').sample(frac=sample_frac, random_state=seed)
            
            # Copia apenas o necessário
            df_amostra = df_input[df_input['EstacaoCodigo'].isin(estacoes_sorteadas['EstacaoCodigo'])].copy()
            
            # Blindagem 1
            df_amostra = df_amostra.drop_duplicates(subset=['EstacaoCodigo', 'Ano', 'Mes'])
            
            # 2. MELT
            colunas_chuva = [f'Chuva{i:02d}' for i in range(1, 32)]
            colunas_existentes = [c for c in colunas_chuva if c in df_amostra.columns]
            
            df_melted = df_amostra.melt(
                id_vars=['EstacaoCodigo', 'Ano', 'Mes'], 
                value_vars=colunas_existentes,
                var_name='Dia_str',
                value_name='Valor'
            )
            
            del df_amostra 
            
            # Limpeza
            df_melted = df_melted.dropna(subset=['Valor'])
            df_melted['Dia'] = df_melted['Dia_str'].str.replace('Chuva', '').astype(int)
            
            # Data Real
            df_melted['Data_Real'] = pd.to_datetime(
                dict(year=df_melted.Ano, month=df_melted.Mes, day=df_melted.Dia), 
                errors='coerce'
            )
            df_melted = df_melted.dropna(subset=['Data_Real'])
            
            # Blindagem 2
            df_melted = df_melted.drop_duplicates(subset=['EstacaoCodigo', 'Data_Real'])
            
            # 3. CÁLCULO
            df_resumo = df_melted.groupby(['EstacaoCodigo', 'Ano']).size().to_frame(name='dias_com_dados').reset_index()
            del df_melted

            # Calcula %
            df_resumo['total_dias_ano'] = df_resumo['Ano'].apply(get_dias_ano_teste)
            df_resumo['Completude_%'] = (df_resumo['dias_com_dados'] / df_resumo['total_dias_ano']) * 100
            
            # 4. RESULTADO
            media_geral = df_resumo['Completude_%'].mean()
            
            resultados_temp.append({
                'Random_State': seed,
                'Media_Completude_%': media_geral
            })
            
        return pd.DataFrame(resultados_temp)

    # Chama a função cacheada
    df_comparacao = executar_teste_estabilidade(df, porcentagem_amostra)

    # Exibe resultados
    st.subheader("Resumo das 5 Simulações")
    st.dataframe(df_comparacao, use_container_width=True)
    
    amplitude = df_comparacao['Media_Completude_%'].max() - df_comparacao['Media_Completude_%'].min()
    
    col1, col2 = st.columns(2)
    col1.metric("Amplitude (Max - Min)", f"{amplitude:.4f} p.p.")
    
    if amplitude < 1.0:
        col2.success("✅ CONCLUSÃO: Sua amostragem é MUITO ESTÁVEL. Pode confiar em rodar apenas uma vez.")
    else:
        col2.warning("⚠️ CONCLUSÃO: Há variabilidade. Considere aumentar a porcentagem da amostra.")

else:
    st.info("👈 Por favor, carregue um arquivo CSV na barra lateral ou use o caminho local padrão.")