"""
Created on 11/25/2020
@author: thiago.rodrigues

"""
import datetime
import requests
import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text

# iniciando consulta webservice
print("[{time}] -----------------Organizze - Invoices (Update) -----------------".format
      (time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

# crendenciais
Username = "seu@email"
Password = "token"

# dados necessarios no header da requisicao
header = {
    'User-Agent': Username,
    'Password': Password,
    'Content-Type': 'application/json'
}

# body requisição inicial
payload = {}

# criando o array de informacoes
id_invoice = []
due_date = []
amount_cents = []
credit_card_id = []

# Variáveis de data
data_inicio = datetime.date(2019, 1, 1)
data_fim = (datetime.date.today() + relativedelta(months=1))
print(f"Data Inicio: {data_inicio}")
print(f"Data Fim: {data_fim}")

# Criando lista de datas
date_ranges = []

i = j = data_inicio

while j < data_fim:
    j += relativedelta(months=1)
    if j > data_fim:
        j = data_fim
    date_ranges.append({"start_date": i, "end_date": j})
    i = j

# Iteração por data
for date_range in date_ranges:
    site = f"https://api.organizze.com.br/rest/v2/credit_cards/204677/invoices?start_date={date_range['start_date']}&end_date={date_range['end_date']}"

    # requisicao ao webservice
    r = requests.request("GET", url=site, headers=header, auth=(Username, Password), data=payload)

    # tratando response
    req_status_json = r.json()

    # Extraindo os valores dos campos da API e appendando nas arrays
    for item in req_status_json:
        id_invoice.append(item['id'])
        due_date.append(item['date'])
        amount_cents.append(item['amount_cents']/100 *-1)
        credit_card_id.append(item['credit_card_id'])

# Criando dataframe
df = pd.DataFrame({
    "id_invoice": id_invoice,
    "due_date": due_date,
    "amount_cents": amount_cents,
    "credit_card_id": credit_card_id
})

# criando dataframe sem duplicatas e com qtd de frequencia de repeticao
df_final = \
    df.groupby(['id_invoice', 'due_date', 'amount_cents', 'credit_card_id']).size().reset_index(name='freq')

# criando engine de conexão SQL
db_organizze = create_engine('mssql+pymssql://{}:{}@{}/{}'.format('user_sql', 'senha_sql', 'host', 'nome_banco'))

# Limpa Base do Dia
print("[{time}] Limpando lancamentos do dia.".format(time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

# SQL para limpar a tabela de dados
apaga_dia = text(""" DELETE FROM tabela_invoices """)

# Executando o comando de limpeza de tabela
db_organizze.execute(apaga_dia)

# Appendando as informações do Data Frame para o SQL
df_final.to_sql(name='tabela_invoices', con=db_organizze, if_exists='append', index=False)

print("[{time}] Dataframe carregado em DB.Organizze".format(
    time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

print("[{time}] Conexoes encerradas".format(time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
