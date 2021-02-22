"""
Created on 12/8/2020
@author: thiago.rodrigues

"""

import datetime
import requests
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text


print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] "
      f"----------------- Organizze - Lancamentos (Update) -----------------")

# Variáveis de data
# data_anterior = date.today() - relativedelta(months=2)
# data_inicio = data_anterior
# data_fim = date.today() + relativedelta(months=1)
data_inicio = datetime.date(2020, 1, 1)
data_fim = date.today()
print(f"Data Inicio: {data_inicio}")
print(f"Data Fim: {data_fim}")

# Token
username = "seu@email"
password = "seu_token"

# Dados do header
header = {
    'User-Agent': username,
    'Password': password,
    'Content-Type': 'application/json'
}

# Body de requisicao inicial
payload = {}

# Criando lista
id_transaction = []
description = []
data_compra = []
valor = []
total_parcelas = []
parcela = []
account_id = []
category_id = []
note = []
credit_card_id = []
credit_card_invoice_id = []
created_at = []
updated_at = []

# Lista de datas
date_ranges = []

i = j = data_inicio

# Loop de extracao do range de datas
while j < data_fim:
    j += relativedelta(months=1)
    if j > data_fim:
        j = data_fim
    date_ranges.append({"start_date": i, "end_date": j})
    i = j

# Iteração por data
for date_range in date_ranges:
    site = f"https://api.organizze.com.br/rest/v2/transactions?start_date={date_range['start_date']}&end_date={date_range['end_date']}"

    # Requisicao ao webservice
    r = requests.request("GET", url=site, headers=header, auth=(username, password), data=payload)

    # Tratando response
    req_status_json = r.json()

    # Loop de extracao dos valores
    for item in req_status_json:
        id_transaction.append(item['id'])
        description.append(item['description'])
        data_compra.append(item['date'])
        valor.append(item['amount_cents']/100 *-1)
        total_parcelas.append(item['total_installments'])
        parcela.append(item['installment'])
        account_id.append(item['account_id'])
        category_id.append(item['category_id'])
        note.append(item['notes'])
        credit_card_id.append(item['credit_card_id'])
        credit_card_invoice_id.append(item['credit_card_invoice_id'])
        created_at.append(item['created_at'])
        updated_at.append(item['updated_at'])

# Criando dataframe
df = pd.DataFrame({
    "id_transaction": id_transaction,
    "description": description,
    "data_compra": data_compra,
    "valor": valor,
    "parcela": parcela,
    "total_parcelas" : total_parcelas,
    "account_id": account_id,
    "category_id": category_id,
    "note": note,
    "credit_card_id": credit_card_id,
    "credit_card_invoice_id": credit_card_invoice_id,
    "created_at": created_at,
    "updated_at": updated_at})

print(f"Quantidade total de registros: {df.shape[0]}")

# Criando engine de conexão SQL
db_organizze = create_engine('mssql+pymssql://{}:{}@{}/{}'.format('usuario_sql', 'senha_sql', 'host', 'nome_banco'))

# print(f"Realizando limpeza do banco de dados")
apaga_dia = text(f""" DELETE FROM nome_da_tabela_sql WHERE data_compra BETWEEN '{data_inicio}' AND '{data_fim}' """)

# Executando o comando da limpeza do banco
db_organizze.execute(apaga_dia)

# Extraindo as informações do Dataframe (tb_transactions) para o SQL
df.to_sql(name='tb_transactions', con=db_organizze, if_exists='append', index=False)

print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Dataframe carregado em ORGANIZZE")
