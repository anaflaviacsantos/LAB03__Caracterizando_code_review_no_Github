import requests
import json
import csv
import os
import time
from datetime import datetime, timezone
from dateutil import parser
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

BASE_URL = "https://api.github.com/graphql"
ACCESS_TOKEN = os.getenv("TOKEN")
OUTPUT_FILENAME = "../results/pull_requests_dataset_final.csv"

if not ACCESS_TOKEN:
  print("ERRO: Token não encontrado. Verifique seu arquivo .env.")
  exit()

REQUEST_HEADERS = {"Authorization": f"token {ACCESS_TOKEN}"}

QUERY_PRS = """
query($owner: String!, $name: String!, $cursor: String) {
repository(owner: $owner, name: $name) {
  pullRequests(states: [MERGED, CLOSED], first: 100, after: $cursor) { # Otimização #1
    pageInfo {
      endCursor
      hasNextPage
    }
    nodes {
      number; changedFiles; additions; deletions; createdAt; closedAt;
      mergedAt; body;
      participants(first: 1) { totalCount }
      totalCommentsCount
      reviews(first: 1) { totalCount }
    }
  }
}
rateLimit { remaining; resetAt }
}
"""

# Função para fazer requisições à API do GitHub com tratamento de erros e tentativas
# Adicionado timeout para evitar travamentos
#   param query: A query GraphQL a ser executada.
def request(query, variables={}, retries=3, delay=5):
  for attempt in range(retries):
      try:
          response = requests.post(
              url=BASE_URL, headers=REQUEST_HEADERS,
              json={"query": query, "variables": variables}, timeout=30
          )
          if response.status_code == 200:
              json_response = response.json()
              if 'errors' in json_response:
                  print(f"  Erro na API do GitHub: {json_response['errors']}")
                  if any("RATE_LIMITED" in e.get('type', '') for e in json_response['errors']):
                      print("  Pausando devido ao limite de taxa. Aguardando 60 segundos...")
                      time.sleep(60)
                      continue 
                  return None
              return json_response
          else:
              print(f"  Falha na requisição (Tentativa {attempt + 1}/{retries}): Status {response.status_code}")
      except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
          print(f"  Erro de conexão/JSON, tentativa {attempt + 1}/{retries}): {e}")
      
      print(f"  Aguardando {delay}s para tentar novamente...")
      time.sleep(delay)
      delay *= 2 
  return None

# Função para ler o CSV de saída e retornar um set com os repositórios já processados
# Otimização: Evitar reprocessar repositórios já feitos
#  param filename: Nome do arquivo CSV de saída.
def getValidRepos(filename):
  if not os.path.exists(filename):
      return set()
  try:
      df = pd.read_csv(filename, usecols=['repo_owner', 'repo_name'])
      processed = set(df.apply(lambda row: f"{row['repo_owner']}/{row['repo_name']}", axis=1))
      print(f"Encontrados {len(processed)} repositórios já processados no arquivo de saída.")
      return processed
  except (pd.errors.EmptyDataError, KeyError):
      return set()

# Função para processar um único repositório e coletar todos os PRs válidos
# Aplica os filtros necessários e coleta as métricas
# Retorna uma lista de dicionários com os dados dos PRs válidos
#  param owner: Dono do repositório.
#  param name: Nome do repositório.
def processRepo(owner, name):
  valid_prs_for_this_repo = []
  cursor = None
  has_next_page = True
  page_count = 0

  while has_next_page:
      page_count += 1
      variables = {'owner': owner, 'name': name, 'cursor': cursor}
      response_json = request(QUERY_PRS, variables)

      if not response_json or not response_json.get('data', {}).get('repository'):
          print("  Não foi possível obter dados. Pulando para o próximo repositório.")
          break

      repo_data = response_json['data']['repository']
      rate_limit = response_json.get('data', {}).get('rateLimit', {})
      
      print(f"  [Página: {page_count} | API: {rate_limit.get('remaining', 'N/A')}] Processando PRs...")

      if not repo_data or not repo_data.get('pullRequests'):
          print("  Repositório sem pull requests ou inacessível. Pulando.")
          break
          
      prs_data = repo_data['pullRequests']

      for pr in prs_data['nodes']:
          if pr['reviews']['totalCount'] == 0: continue
          try:
              created_at = parser.parse(pr['createdAt'])
              closed_time_str = pr['mergedAt'] if pr['mergedAt'] else pr['closedAt']
              if not closed_time_str: continue
              closed_at = parser.parse(closed_time_str)
              if (closed_at - created_at).total_seconds() < 3600: continue
          except (TypeError, ValueError): continue

          this_pr = {
              'repo_owner': owner, 'repo_name': name, 'pr_number': pr['number'],
              'changed_files': pr['changedFiles'], 'additions': pr['additions'],
              'deletions': pr['deletions'], 'created_at': pr['createdAt'],
              'closed_at': pr['closedAt'], 'merged_at': pr['mergedAt'],
              'body_chars': len(pr['body']) if pr['body'] else 0,
              'participants': pr['participants']['totalCount'],
              'comments': pr['totalCommentsCount'], 'reviews': pr['reviews']['totalCount']
          }
          valid_prs_for_this_repo.append(this_pr)
      
      has_next_page = prs_data['pageInfo']['hasNextPage']
      cursor = prs_data['pageInfo']['endCursor']

  return valid_prs_for_this_repo


def main():
  repos_df = pd.read_csv("../results/repos.csv")
  processed_repos = getValidRepos(OUTPUT_FILENAME) # Otimização #3

  for index, repo in repos_df.iterrows():
      owner, name = repo['owner'], repo['name']
      repo_full_name = f"{owner}/{name}"

      print(f"\n--- Processando Repositório {index + 1}/{len(repos_df)}: {repo_full_name} ---")

      if repo_full_name in processed_repos:
          print("  Repositório já processado")
          continue

      repo_prs = processRepo(owner, name)

      if repo_prs:
          print(f"  Coletados {len(repo_prs)} PRs válidos para {repo_full_name}.")
          # Otimização: Salvar incrementalmente
          file_exists = os.path.exists(OUTPUT_FILENAME)
          with open(OUTPUT_FILENAME, 'a', newline='', encoding='utf-8') as csvfile:
              writer = csv.DictWriter(csvfile, fieldnames=repo_prs[0].keys())
              if not file_exists or os.path.getsize(OUTPUT_FILENAME) == 0:
                  writer.writeheader()
              writer.writerows(repo_prs)
          print(f"  Dados de {repo_full_name} salvos em '{OUTPUT_FILENAME}'.")
      else:
          print(f"  Nenhum PR válido encontrado para {repo_full_name}.")

  print("Coleta concluída.")

if __name__ == "__main__":
  main()
