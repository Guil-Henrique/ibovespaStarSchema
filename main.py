import os
import csv
import psycopg2
from psycopg2 import sql
from datetime import datetime

## criar db
def create_database_if_not_exists(db_name):
    try:
        conn = psycopg2.connect(
            dbname='postgres', user=db_user, password=db_password, host=db_host, port=db_port
        )
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}';")
        exists = cur.fetchone()
        
        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(db_name)))
            print(f"{db_name} criado com sucesso.")
        else:
            print(f"{db_name} já existe.")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"erro: {e}")

## info do db
db_user = os.getenv('POSTGRES_USER')
db_password = os.getenv('POSTGRES_PASSWORD')
db_host = os.getenv('POSTGRES_HOST')
db_port = os.getenv('POSTGRES_PORT')
db_name = os.getenv('POSTGRES_DB')

create_database_if_not_exists(db_name)
conn = psycopg2.connect(
    dbname=db_name, 
    user=db_user, 
    password=db_password,
    host=db_host, 
    port=db_port
)
cur = conn.cursor()

##tabela empresa
create_empresa_table = '''
CREATE TABLE IF NOT EXISTS empresa (
    id SERIAL PRIMARY KEY,
    nome_empresa VARCHAR(255) NOT NULL UNIQUE,
    ticker VARCHAR(10) NOT NULL UNIQUE,
    setor VARCHAR(50) NOT NULL,
    receita_liquida_trimestral NUMERIC
);
'''
cur.execute(create_empresa_table)
conn.commit()

def insert_empresa(nome_empresa, ticker, setor, receita_liquida):
    cur.execute('SELECT id FROM empresa WHERE nome_empresa = %s;', (nome_empresa,))
    result = cur.fetchone()
    
    if result is None:
        cur.execute(''' 
            INSERT INTO empresa (nome_empresa, ticker, setor, receita_liquida_trimestral) 
            VALUES (%s, %s, %s, %s) RETURNING id;
        ''', (nome_empresa, ticker, setor, receita_liquida))
        return cur.fetchone()[0]
    else:
        return result[0]

## tabela calendario
create_calendario_table = '''
CREATE TABLE IF NOT EXISTS calendario (
    id SERIAL PRIMARY KEY,
    data DATE NOT NULL UNIQUE,
    ano INT,
    mes INT,
    dia INT
);
'''
cur.execute(create_calendario_table)
conn.commit()

## tabela acoes (FATO)
create_acoes_table = '''
CREATE TABLE IF NOT EXISTS acoes (
    id SERIAL PRIMARY KEY,
    empresa_id INT REFERENCES empresa(id),
    calendario_id INT REFERENCES calendario(id),
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume BIGINT,
    variacao_preco NUMERIC,
    UNIQUE(empresa_id, calendario_id)
);
'''
cur.execute(create_acoes_table)
conn.commit()

##csv
csv_folder = './dadosCsv'

def insert_or_get_calendario(data_str):
    data = datetime.strptime(data_str.split()[0], '%Y-%m-%d').date()
    ano = data.year
    mes = data.month
    dia = data.day
    cur.execute('SELECT id FROM calendario WHERE data = %s;', (data,))
    result = cur.fetchone()
    
    if result:
        return result[0]
    else:
        cur.execute('INSERT INTO calendario (data, ano, mes, dia) VALUES (%s, %s, %s, %s) RETURNING id;', (data, ano, mes, dia))
        calendario_id = cur.fetchone()[0]
        conn.commit()
        return calendario_id

##load do csv na tabeela fato
def load_csv_to_db(file_name, empresa_id):
    file_path = os.path.join(csv_folder, file_name)
    
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        
        for row in reader:
            date, open_, high, low, close, volume, dividends, stock_splits = row
            calendario_id = insert_or_get_calendario(date)
            cur.execute('SELECT 1 FROM acoes WHERE empresa_id = %s AND calendario_id = %s;', (empresa_id, calendario_id))
            if cur.fetchone() is None:
                try:
                    cur.execute(''' 
                        SELECT close FROM acoes WHERE empresa_id = %s AND calendario_id = (
                            SELECT id FROM calendario WHERE data = (
                                SELECT data FROM calendario WHERE id = %s) - INTERVAL '1 day'
                        )
                    ''', (empresa_id, calendario_id))
                    previous_close = cur.fetchone()
                    ##preco de variacao
                    if previous_close:
                        previous_close_price = float(previous_close[0])
                        variacao_preco = ((float(close) - previous_close_price) / previous_close_price) * 100
                    else:
                        variacao_preco = None

                    print(f"empresa_id={empresa_id}, calendario_id={calendario_id}, open={open_}, high={high}, low={low}, close={close}, volume={volume}, variacao_preco={variacao_preco}")
                    cur.execute(''' 
                        INSERT INTO acoes (empresa_id, calendario_id, open, high, low, close, volume, variacao_preco)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                    ''', (empresa_id, calendario_id, open_, high, low, close, volume, variacao_preco))
                except Exception as e:
                    print(f"erro: {e}")

    conn.commit()

##csv da tabela empresa, adcionando colunas que não tem nocsv
empresas_csv = {
    'dados_ambev.csv': ('Ambev', 'ABEV3', 'Consumo', 19934225000.00),
    'dados_bbas.csv': ('Banco do Brasil', 'BBAS3', 'Serviços Financeiros', 35198141750.00),
    'dados_bradesco.csv': ('Bradesco', 'BBDC4', 'Serviços Financeiros', 25068553000.00),
    'dados_braskem.csv': ('Braskem', 'BRKM5', 'Materiais Básicos', 17642250000.00),
    'dados_eletrobras.csv': ('Eletrobras', 'ELET3', 'Utilidades', 9289727000.00),
    'dados_itau.csv': ('Itaú', 'ITUB4', 'Serviços Financeiros', 37095000000.00),
    'dados_petrobras.csv': ('Petrobras', 'PETR4', 'Energia', 25602250000.00),
    'dados_vale.csv': ('Vale', 'VALE3', 'Materiais Básicos', 10446000000.00),
    'dados_weg.csv': ('Weg', 'WEGE3', 'Indústria', 8125900250.00),
}
for file_name, (empresa_nome, ticker, setor, receita_liquida) in empresas_csv.items():
    empresa_id = insert_empresa(empresa_nome, ticker, setor, receita_liquida)
    load_csv_to_db(file_name, empresa_id)


cur.close()
conn.close()
