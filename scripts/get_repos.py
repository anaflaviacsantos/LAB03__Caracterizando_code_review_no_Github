import requests
import json
import csv
import os
import time
from dotenv import load_dotenv

print(f"Pasta de trabalho atual: {os.getcwd()}") # Adicione esta linha
load_dotenv()

BASE_URL = "https://api.github.com/graphql"
ACCESS_TOKEN = os.getenv("TOKEN")
print(f"Token carregado: {ACCESS_TOKEN}")
REQUEST_HEADERS = {"Authorization": f"token {ACCESS_TOKEN}"}


QUERY_SEARCH_REPOS = """
query($cursor: String) {
search(query: "stars:>1 sort:stars-desc", type: REPOSITORY, first: 10, after: $cursor) {
  pageInfo {
    endCursor
    hasNextPage
  }
  edges {
    node {
      ... on Repository {
        name
        owner {
          login
        }
      }
    }
  }
}
rateLimit {
  cost
  remaining
  resetAt
}
}
"""

QUERY_CHECK_PRS = """
query($owner: String!, $name: String!) {
repository(owner: $owner, name: $name) {
  # Adicionando o filtro para contar apenas PRs fechados ou mergidos
  pullRequests(states: [MERGED, CLOSED]) {
    totalCount
  }
}
}
"""

# Função para fazer requisições à API do GitHub
#   param query: A query GraphQL a ser executada.
#   param variables: Variáveis para a query.
def request(query, variables={}):
  response = requests.post(
      url=BASE_URL,
      headers=REQUEST_HEADERS,
      json={"query": query, "variables": variables}
  )
  if response.status_code == 200:
      json_response = json.loads(response.content.decode('utf-8'))
      if 'errors' in json_response:
          print("Erro na API do GitHub:", json_response['errors'])
          return None
      return json_response
  else:
      print(f"Falha na requisição: {response.status_code} - {response.text}")
      return None

# Função para checar se um repositório tem pelo menos 100 PRs
#   param owner: Dono do repositório.
#   param name: Nome do repositório.
def countPRs(owner, name):
  variables = {"owner": owner, "name": name}
  json_response = request(QUERY_CHECK_PRS, variables)
  
  if json_response and json_response.get('data', {}).get('repository'):
      pr_count = json_response['data']['repository']['pullRequests']['totalCount']
      return pr_count >= 100
  return False


# Função para buscar os primeiros 200 repositórios com mais de 100 PRs
# Chama a função request para buscar repositórios e countPRs para validar cada um
# Retorna uma lista de dicionários com 'name' e 'owner' dos repositórios válidos
# Usa time.sleep(0.5) para evitar atingir o limite de taxa da API
def getValidRepos():
  valid_repos = []
  cursor = None
  invalid_repos = 0
  
  print("Iniciando a busca")

  while len(valid_repos) < 200:
      search_vars = {"cursor": cursor}
      search_response = request(QUERY_SEARCH_REPOS, search_vars)

      if not search_response or not search_response.get('data', {}).get('search'):
          print("Não foi possível buscar mais repositórios")
          break

      repos = search_response['data']['search']['edges']
      page_info = search_response['data']['search']['pageInfo']
      rate_limit = search_response.get('data', {}).get('rateLimit', {})
      
      print(f"API Rate Limit restante: {rate_limit.get('remaining', 'N/A')}")

      for repo_edge in repos:
          node = repo_edge['node']
          owner = node['owner']['login']
          name = node['name']
          
          print(f"Verificando {owner}/{name}...", end=" ")
          time.sleep(0.5)

          if countPRs(owner, name):
              print("Válido")
              this_repo = {'name': name, 'owner': owner}
              valid_repos.append(this_repo)
              print(f"Repositórios encontrados: {len(valid_repos)}/200")
              if len(valid_repos) >= 200:
                  break
          else:
              print("Inválido.")
              invalid_repos += 1
              print(f"{invalid_repos} inválidos")

      if not page_info['hasNextPage']:
          print("Busca concluída")
          break
      
      cursor = page_info['endCursor']

  return valid_repos

# Função para salvar a lista de repositórios válidos em um arquivo CSV
#   param list_of_repos: Lista de dicionários com 'name' e 'owner' dos repositórios.
def saveToCSV(list_of_repos):
  filename = "results/repos.csv"
  fields = ["name", "owner"]
  
  os.makedirs(os.path.dirname(filename), exist_ok=True)

  with open(filename, 'w', newline='') as csvfile:
      writer = csv.DictWriter(csvfile, fieldnames=fields)
      writer.writeheader()
      writer.writerows(list_of_repos)
  print(f"\nArquivo '{filename}' salvo com sucesso com {len(list_of_repos)} repositórios.")

if __name__ == "__main__":
  final_list = getValidRepos()
  if final_list:
      saveToCSV(final_list)