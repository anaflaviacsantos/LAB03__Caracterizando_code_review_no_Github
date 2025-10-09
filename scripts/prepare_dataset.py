import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import spearmanr
import os 


DATASET_PATH = "results/final_dataset.csv"

def load_and_prepare_data(filepath):
    """
    Carrega o dataset de PRs, limpa e cria as colunas necessárias para a análise.
    """
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        print(f"ERRO: Arquivo '{filepath}' não encontrado.")
        return None

    # Conversão de datas e remoção de linhas com erro
    for col in ['created_at', 'closed_at', 'merged_at']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    df.dropna(subset=['created_at', 'closed_at'], inplace=True)

    # Criação de novas colunas para as métricas
    df['is_merged'] = np.where(df['merged_at'].notna(), 1, 0)
    df['analysis_duration_hours'] = (df['closed_at'] - df['created_at']).dt.total_seconds() / 3600
    df['total_changes'] = df['additions'] + df['deletions']

    print(f"Dataset carregado e preparado com {len(df)} linhas válidas.")
    return df

def analyze_correlation(df, col1, col2, rq_id):
    """
    Executa e interpreta o teste de correlação de Spearman para duas colunas.
    """
    print(f"\n--- Análise da {rq_id} ({col1} vs. {col2}) ---")

    # Garante que não há valores infinitos que podem quebrar o teste
    df_filtered = df.replace([np.inf, -np.inf], np.nan).dropna(subset=[col1, col2])

    if len(df_filtered) < 2:
        print("Não há dados suficientes para realizar a análise.")
        return

    rho, p_value = spearmanr(df_filtered[col1], df_filtered[col2])

    print(f"Coeficiente de Correlação (Rho): {rho:.4f}")
    print(f"P-value: {p_value:.4f}")

    if p_value < 0.05:
        print("Resultado: Estatisticamente Significante.")
        if abs(rho) >= 0.5: strength = "forte"
        elif abs(rho) >= 0.3: strength = "moderada"
        else: strength = "fraca"
        
        if rho > 0: direction = "positiva"
        else: direction = "negativa"
        
        print(f"Interpretação: Há uma correlação {strength} e {direction}.")
    else:
        print("Resultado: Não é Estatisticamente Significante.")
        print("Interpretação: Não podemos afirmar que existe uma correlação real.")

def run_all_analyses(df):
    """
    Orquestra a execução da análise para todas as 8 Questões de Pesquisa.
    """
    print("\n\n--- INICIANDO ANÁLISE ESTATÍSTICA ---")

    # Dimensão A: Feedback Final das Revisões (Status do PR)
    analyze_correlation(df, 'total_changes', 'is_merged', 'RQ01')
    analyze_correlation(df, 'analysis_duration_hours', 'is_merged', 'RQ02')
    analyze_correlation(df, 'body_chars', 'is_merged', 'RQ03')
    analyze_correlation(df, 'comments', 'is_merged', 'RQ04') # Usando 'comments' para 'Interações'

    # Dimensão B: Número de Revisões
    analyze_correlation(df, 'total_changes', 'reviews', 'RQ05')
    analyze_correlation(df, 'analysis_duration_hours', 'reviews', 'RQ06')
    analyze_correlation(df, 'body_chars', 'reviews', 'RQ07')
    analyze_correlation(df, 'comments', 'reviews', 'RQ08') # Usando 'comments' para 'Interações'
    
def generate_visualizations(df):
    """
    Gera e SALVA os gráficos para as 8 Questões de Pesquisa em uma pasta 'results/images'.
    """
    print("\n\n--- INICIANDO GERAÇÃO E SALVAMENTO DE VISUALIZAÇÕES ---")
    sns.set_theme(style="whitegrid")

    # Cria o diretório para salvar as imagens, se não existir
    output_dir = "results/images"
    os.makedirs(output_dir, exist_ok=True)
    print(f"As imagens serão salvas em: '{output_dir}'")

    # --- Gráficos para Dimensão A (RQs 01-04): Boxplots ---
    metrics_a = [
        ('total_changes', 'RQ01_Tamanho_vs_Status'),
        ('analysis_duration_hours', 'RQ02_Tempo_vs_Status'),
        ('body_chars', 'RQ03_Descricao_vs_Status'),
        ('comments', 'RQ04_Interacoes_vs_Status')
    ]

    for metric, filename in metrics_a:
        plt.figure(figsize=(10, 6))
        sns.boxplot(x='is_merged', y=metric, data=df, showfliers=False)
        plt.title(f"{filename.replace('_', ' ')}", fontsize=16)
        plt.xlabel("Status do PR", fontsize=12)
        plt.ylabel(metric, fontsize=12)
        plt.xticks([0, 1], ['Fechado (Closed)', 'Mergido (Merged)'])
        
        # Define o caminho completo do arquivo de saída
        filepath = os.path.join(output_dir, f"{filename}.png")
        
        # Salva a figura em vez de exibi-la
        plt.savefig(filepath, bbox_inches='tight')
        plt.close() # Fecha a figura para liberar memória
        print(f"  - Gráfico salvo: {filepath}")

    # --- Gráficos para Dimensão B (RQs 05-08): Hexbin ---
    metrics_b = [
        ('total_changes', 'reviews', 'RQ05_Tamanho_vs_Revisoes'),
        ('analysis_duration_hours', 'reviews', 'RQ06_Tempo_vs_Revisoes'),
        ('body_chars', 'reviews', 'RQ07_Descricao_vs_Revisoes'),
        ('comments', 'reviews', 'RQ08_Interacoes_vs_Revisoes')
    ]

    df_sample = df.sample(n=min(5000, len(df)), random_state=42)

    for metric1, metric2, filename in metrics_b:
        g = sns.jointplot(x=metric1, y=metric2, data=df_sample, kind="hex", height=8)
        g.fig.suptitle(f"{filename.replace('_', ' ')}", y=1.02, fontsize=16)
        
        # Define o caminho completo do arquivo de saída
        filepath = os.path.join(output_dir, f"{filename}.png")
        
        # Salva a figura em vez de exibi-la
        plt.savefig(filepath, bbox_inches='tight')
        plt.close() # Fecha a figura para liberar memória
        print(f"  - Gráfico salvo: {filepath}")

if __name__ == "__main__":

    df_prepared = load_and_prepare_data(DATASET_PATH)

    if df_prepared is not None:
        run_all_analyses(df_prepared)
        
        # Adicione a chamada para a nova função aqui
        generate_visualizations(df_prepared)
        
        print("\n\n--- ANÁLISE E VISUALIZAÇÃO CONCLUÍDAS ---")