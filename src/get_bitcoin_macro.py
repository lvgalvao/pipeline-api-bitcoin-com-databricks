# Databricks notebook source
# MAGIC %md
# MAGIC # Extração de Dados Bitcoin - Workflow Macro
# MAGIC 
# MAGIC Script simplificado para uso em workflows do Databricks.
# MAGIC Extrai dados da API Coinbase, converte para BRL e salva em Delta Table.

# COMMAND ----------

import requests
from datetime import datetime

# COMMAND ----------

# Criar widget para receber parâmetros do pipeline (Key-Value)
# O valor será passado através da configuração Key-Value do pipeline
dbutils.widgets.text("api_key", "", "API Key CurrencyFreaks")

# COMMAND ----------

def extrair_dados_bitcoin():
    """Extrai o JSON completo da API da Coinbase."""
    url = 'https://api.coinbase.com/v2/prices/spot'
    resposta = requests.get(url)
    return resposta.json()

def extrair_cotacao_usd_brl():
    """Extrai a cotação USD-BRL da API CurrencyFreaks."""
    # Buscar API key dos parâmetros do pipeline (Key-Value)
    # Os valores configurados no pipeline são acessíveis via widgets
    api_key = dbutils.widgets.get("api_key")
    url = f'https://api.currencyfreaks.com/v2.0/rates/latest?apikey={api_key}'
    resposta = requests.get(url)
    return resposta.json()

def tratar_dados_bitcoin(dados_json, taxa_usd_brl):
    """Transforma os dados brutos da API, renomeia colunas, adiciona timestamp e converte para BRL."""
    valor_usd = float(dados_json['data']['amount'])
    criptomoeda = dados_json['data']['base']
    moeda_original = dados_json['data']['currency']
    
    # Convertendo de USD para BRL
    valor_brl = valor_usd * taxa_usd_brl
    
    # Adicionando timestamp como datetime object
    timestamp = datetime.now()
    
    dados_tratados = [{
        "valor_usd": valor_usd,
        "valor_brl": valor_brl,
        "criptomoeda": criptomoeda,
        "moeda_original": moeda_original,
        "taxa_conversao_usd_brl": taxa_usd_brl,
        "timestamp": timestamp
    }]
    
    return dados_tratados

# COMMAND ----------

# Extraindo dados
Dados_bitcoin = extrair_dados_bitcoin()
dados_cotacao = extrair_cotacao_usd_brl()

# Extraindo a taxa de conversão USD-BRL
taxa_usd_brl = float(dados_cotacao['rates']['BRL'])

# Tratando os dados e convertendo para BRL
dados_bitcoin_tratado = tratar_dados_bitcoin(Dados_bitcoin, taxa_usd_brl)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS pipeline_api_bitcoin
# MAGIC COMMENT 'Catálogo de demonstração criado para o workshop de pipeline_api_bitcoin';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS pipeline_api_bitcoin.bitcoin_data
# MAGIC COMMENT 'Schema para armazenar dados de Bitcoin processados';

# COMMAND ----------

# Criar Spark DataFrame
df = spark.createDataFrame(dados_bitcoin_tratado)

# COMMAND ----------

# Caminho da tabela Delta no Unity Catalog
delta_table_path = "pipeline_api_bitcoin.bitcoin_data.bitcoin_data"

# Salvar como Delta Table (modo append se a tabela já existir)
df.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(delta_table_path)

