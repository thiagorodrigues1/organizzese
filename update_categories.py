"""
Created on 12/8/2020
@author: thiago.rodrigues

"""

import datetime
import json
import requests
import pandas as pd
from sqlalchemy import create_engine, text


#iniciando consulta webservice
print("[{time}] ----------------- Cadastro de Categorias -----------------".format
      (time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

print("[{time}] Inicializando consulta".format
      (time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

# endereco relatorio webservice
site = "https://api.organizze.com.br/rest/v2/categories"

# crendenciais
Username = "seu@gmail"
Password = "token"

# dados necessarios no header da requisicao
header = {
    'User-Agent': Username,
    'Password': Password,
    'Content-Type': 'application/json'
}

# body requisição incial
payload = {}

# requisicao ao webservice
r = requests.request("GET", url=site, headers=header, auth=(Username, Password), data=payload)

# tratando response
req_status_json = r.json()

#print(req_status_json)

# criando o array de informações
id = []
name = []
color = []
parent_id = []

for item in req_status_json:
    id.append(item['id'])
    color.append(item['color'])
    name.append(item['name'])
    parent_id.append(item['parent_id'])

#criando dataframe
df = pd.DataFrame({
    "id": id,
    "name": name,
    "color": color,
    "parent_id": parent_id})

print(f"Quantidade total de registros: {df.shape[0]}")

print("[{time}] Consulta finalizada".format(time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

print("[{time}] Inicializando engine DB.Organizze.".format(
    time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

# criando engine de conexão SQL
db_organizze = create_engine('mssql+pymssql://{}:{}@{}/{}'.format('usuario', 'senha_sql', 'host', 'nome_banco'))

# Limpa Base do Dia
print("[{time}] Limpando lancamentos do dia.".format(time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

# SQL para limpar a tabela de dados
apaga_dia = text(""" DELETE FROM tabela_categories """)

# Executando o comando de limpeza de tabela
db_organizze.execute(apaga_dia)

# Extraindo as informações do Data Frame para o SQL
df.to_sql(name='tabela_categories', con=db_organizze, if_exists='append', index=False)

print("[{time}] Dataframe carregado em DB.Organizze".format(
    time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
