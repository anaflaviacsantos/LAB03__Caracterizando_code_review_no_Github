import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import spearmanr
import os 


DATASET_PATH = "results/final_dataset.csv"

# Função para carregar e preparar o dataset para análise
# Converte datas, cria colunas adicionais e remove linhas inválidas
#   param filepath: Caminho do arquivo CSV do dataset.
#   return: DataFrame preparado.
def prepareData(filepath):
    try:
        df = pd.read_csv(filepath)
    except FileNotFoundError:
        print(f"Arquivo não encontrado.")
        return None

    for col in ['created_at', 'closed_at', 'merged_at']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    df.dropna(subset=['created_at', 'closed_at'], inplace=True)

    df['is_merged'] = np.where(df['merged_at'].notna(), 1, 0)
    df['analysis_duration_hours'] = (df['closed_at'] - df['created_at']).dt.total_seconds() / 3600
    df['total_changes'] = df['additions'] + df['deletions']

    print(f"{len(df)} linhas válidas.")
    return df

    
# Função para gerar e salvar gráficos para as 8 Questões de Pesquisa
#   param df: DataFrame preparado.
def generate_visualizations(df):
    
    sns.set_theme(style="whitegrid")

    output_dir = "results/images"
    os.makedirs(output_dir, exist_ok=True)
    
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
        
        
        filepath = os.path.join(output_dir, f"{filename}.png")
        
        plt.savefig(filepath, bbox_inches='tight')
        plt.close() 
        print(f"  - Gráfico salvo: {filepath}")


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
        
        filepath = os.path.join(output_dir, f"{filename}.png")
        
        plt.savefig(filepath, bbox_inches='tight')
        plt.close() 
        print(f"  - Gráfico salvo: {filepath}")

if __name__ == "__main__":

    df_prepared = prepareData(DATASET_PATH)

    if df_prepared is not None:
        generate_visualizations(df_prepared)
        