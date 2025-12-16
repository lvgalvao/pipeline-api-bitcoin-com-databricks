# Databricks notebook source
# MAGIC %md
# MAGIC # 💰 Extração de Dados Bitcoin - API Coinbase
# MAGIC
# MAGIC Este notebook demonstra como:
# MAGIC - Extrair dados da API da Coinbase
# MAGIC - Converter USD para BRL usando API de economia
# MAGIC - Salvar dados em JSON, CSV e Parquet usando PySpark
# MAGIC - Salvar como **Delta Table** no Databricks
# MAGIC - Converter Delta Table para DataFrame
# MAGIC - Trabalhar com **Unity Catalog** (Catalog, Schema, Volume)
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importando Bibliotecas Necessárias

# COMMAND ----------

import requests
import pandas as pd
from datetime import datetime

# No Databricks, o Spark já está disponível como 'spark', não precisa criar SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extraindo e Transformando Dados

# COMMAND ----------

def extrair_dados_bitcoin():
    """Extrai o JSON completo da API da Coinbase."""
    url = 'https://api.coinbase.com/v2/prices/spot'
    resposta = requests.get(url)
    return resposta.json()

def extrair_cotacao_usd_brl():
    """Extrai a cotação USD-BRL da API CurrencyFreaks."""
    api_key = 'bf8ddcbf744d4e7897e836941c32e39f'
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

# Extraindo dados
Dados_bitcoin = extrair_dados_bitcoin()
dados_cotacao = extrair_cotacao_usd_brl()

# Extraindo a taxa de conversão USD-BRL
taxa_usd_brl = float(dados_cotacao['rates']['BRL'])

# Tratando os dados e convertendo para BRL
dados_bitcoin_tratado = tratar_dados_bitcoin(Dados_bitcoin, taxa_usd_brl)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configurando Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS pipeline_api_bitcoin
# MAGIC COMMENT 'Catálogo de demonstração criado para o workshop de pipeline_api_bitcoin';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS pipeline_api_bitcoin.lakehouse
# MAGIC COMMENT 'Schema Lakehouse para salvar dados processados';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS pipeline_api_bitcoin.lakehouse.raw_files
# MAGIC COMMENT 'Volume para arquivos brutos de ingestão inicial';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS pipeline_api_bitcoin.bitcoin_data
# MAGIC COMMENT 'Schema para armazenar dados de Bitcoin processados';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criando DataFrame Pandas
# MAGIC
# MAGIC Vamos criar um DataFrame Pandas para salvar os arquivos de forma simples.

# COMMAND ----------

# Criar DataFrame Pandas a partir dos dados tratados
df_pandas = pd.DataFrame(dados_bitcoin_tratado)

# Mostrar dados
print(df_pandas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salvando em JSON usando Pandas

# COMMAND ----------

# Pega o timestamp do próprio evento
event_ts = dados_bitcoin_tratado[0]["timestamp"]

# Converte para formato seguro para nome de arquivo
ts = event_ts.strftime("%Y%m%d_%H%M%S_%f")

# Caminho do arquivo JSON
json_file = f"/Volumes/pipeline_api_bitcoin/lakehouse/raw_files/bitcoin_{ts}.json"

# Salvar como JSON usando Pandas
df_pandas.to_json(json_file, orient='records', date_format='iso', indent=2)
print(f"✅ Arquivo JSON salvo: {json_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Salvando como CSV usando PySpark
# MAGIC
# MAGIC ### 📄 O que é CSV?
# MAGIC
# MAGIC **CSV (Comma-Separated Values)** é um formato de arquivo **texto** simples e universal:
# MAGIC
# MAGIC **Características Técnicas:**
# MAGIC - ✅ **Formato texto**: Arquivo legível por humanos (pode abrir no Bloco de Notas)
# MAGIC - ✅ **Row-based**: Armazena dados por linha (otimizado para transações OLTP)
# MAGIC - ✅ **Delimitado por vírgulas**: Valores separados por vírgula (ou ponto-e-vírgula)
# MAGIC - ✅ **Universal**: Suportado por praticamente todas as ferramentas (Excel, Python, R, SQL, etc.)
# MAGIC - ✅ **Simples**: Fácil de entender, debugar e inspecionar manualmente
# MAGIC
# MAGIC **Vantagens:**
# MAGIC - ✅ **Legibilidade**: Pode ser aberto no Excel, Bloco de Notas, Google Sheets
# MAGIC - ✅ **Compatibilidade**: Funciona com qualquer ferramenta de dados
# MAGIC - ✅ **Debugging**: Fácil de inspecionar e corrigir manualmente
# MAGIC - ✅ **Portabilidade**: Pode ser compartilhado facilmente
# MAGIC
# MAGIC **Desvantagens:**
# MAGIC - ❌ **Maior tamanho**: Arquivos maiores que Parquet (sem compressão nativa)
# MAGIC - ❌ **Mais lento**: Leitura e escrita mais lentas em grandes volumes
# MAGIC - ❌ **Sem schema**: Não preserva tipos de dados automaticamente (tudo é texto)
# MAGIC - ❌ **Sem compressão**: Arquivos ocupam mais espaço em disco
# MAGIC - ❌ **Limitações**: Não suporta tipos complexos (arrays, objetos aninhados)
# MAGIC
# MAGIC **Quando usar CSV?**
# MAGIC - Dados pequenos ou médios (< 100MB)
# MAGIC - Quando precisa ser legível por humanos
# MAGIC - Integração com Excel ou ferramentas de negócio
# MAGIC - Debugging e inspeção manual dos dados
# MAGIC - Exportação para sistemas que não suportam Parquet
# MAGIC - Prototipagem rápida

# COMMAND ----------

# Caminho do arquivo CSV
csv_file = f"/Volumes/pipeline_api_bitcoin/lakehouse/raw_files/bitcoin_{ts}.csv"

# Salvar como CSV usando Pandas
df_pandas.to_csv(csv_file, index=False)
print(f"✅ Arquivo CSV salvo: {csv_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Salvando como Parquet usando PySpark
# MAGIC
# MAGIC ### 📊 O que é Parquet?
# MAGIC
# MAGIC **Parquet** é um formato de arquivo **binário e columnar** otimizado para Big Data:
# MAGIC
# MAGIC **Características Técnicas:**
# MAGIC - ✅ **Formato binário**: Arquivo não legível por humanos (otimizado para máquinas)
# MAGIC - ✅ **Columnar**: Armazena dados por coluna, não por linha (otimizado para analytics OLAP)
# MAGIC - ✅ **Compressão nativa**: Usa algoritmos de compressão eficientes (Snappy, Gzip, LZ4)
# MAGIC - ✅ **Schema embutido**: Mantém informações sobre tipos de dados automaticamente
# MAGIC - ✅ **Predicate pushdown**: Permite ler apenas colunas necessárias
# MAGIC - ✅ **Estatísticas**: Inclui estatísticas (min, max, null count) por coluna
# MAGIC
# MAGIC **Vantagens:**
# MAGIC - ✅ **Compressão**: Arquivos muito menores que CSV (até 90% de economia)
# MAGIC - ✅ **Performance**: Leitura rápida, especialmente para consultas analíticas
# MAGIC - ✅ **Big Data**: Ideal para processar grandes volumes de dados (terabytes/petabytes)
# MAGIC - ✅ **Schema**: Preserva tipos de dados (int, float, string, date, etc.)
# MAGIC - ✅ **Columnar**: Consultas que precisam de poucas colunas são muito rápidas
# MAGIC - ✅ **Eficiência**: Menor uso de I/O e memória
# MAGIC
# MAGIC **Desvantagens:**
# MAGIC - ❌ **Não é legível por humanos**: Precisa de ferramentas especiais (Pandas, Spark, etc.)
# MAGIC - ❌ **Overhead**: Para dados muito pequenos, o overhead pode não valer a pena
# MAGIC - ❌ **Escrita mais lenta**: A compressão e organização columnar tornam a escrita mais lenta
# MAGIC
# MAGIC **Quando usar Parquet?**
# MAGIC - Grandes volumes de dados (> 100MB)
# MAGIC - Data Lakes e Data Warehouses
# MAGIC - Quando performance é crítica
# MAGIC - Análises analíticas (OLAP)
# MAGIC - Processamento com Spark/PySpark
# MAGIC - Quando economia de espaço é importante
# MAGIC - Consultas que acessam poucas colunas de muitas linhas

# COMMAND ----------

# Caminho do arquivo Parquet
parquet_file = f"/Volumes/pipeline_api_bitcoin/lakehouse/raw_files/bitcoin_{ts}.parquet"

# Salvar como Parquet usando Pandas
df_pandas.to_parquet(parquet_file, index=False)
print(f"✅ Arquivo Parquet salvo: {parquet_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Convertendo Pandas para PySpark e Salvando como Delta Table
# MAGIC
# MAGIC Agora vamos converter o DataFrame Pandas para PySpark para salvar no Delta Table.

# COMMAND ----------

# Converter DataFrame Pandas para PySpark
# type: ignore - spark está disponível no ambiente Databricks
df_spark = spark.createDataFrame(df_pandas)  # noqa: F821

# Mostrar schema
df_spark.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Salvando como Delta Table
# MAGIC
# MAGIC ### 🎯 O que é Delta Table?
# MAGIC
# MAGIC **Delta Lake** é uma camada de armazenamento open-source que traz **ACID transactions** e **time travel** para Data Lakes. Delta Tables são tabelas que usam o formato Delta.
# MAGIC
# MAGIC **Características Técnicas:**
# MAGIC - ✅ **ACID Transactions**: Garante consistência dos dados (Atomicity, Consistency, Isolation, Durability)
# MAGIC - ✅ **Time Travel**: Permite acessar versões anteriores dos dados
# MAGIC - ✅ **Schema Enforcement**: Valida e garante que os dados seguem o schema definido
# MAGIC - ✅ **Upsert/Merge**: Suporta operações de atualização e merge eficientes
# MAGIC - ✅ **Baseado em Parquet**: Usa Parquet como formato físico (herda todas as vantagens)
# MAGIC - ✅ **Transaction Log**: Mantém um log de todas as transações (Delta Log)
# MAGIC - ✅ **Optimize & Z-Order**: Ferramentas para otimizar performance
# MAGIC
# MAGIC **Vantagens sobre Parquet simples:**
# MAGIC - ✅ **Transações ACID**: Múltiplas escritas simultâneas sem corrupção
# MAGIC - ✅ **Time Travel**: Acesse versões históricas dos dados
# MAGIC - ✅ **Schema Evolution**: Permite evoluir o schema ao longo do tempo
# MAGIC - ✅ **Delete/Update**: Suporta operações de atualização e deleção
# MAGIC - ✅ **Auditoria**: Histórico completo de mudanças
# MAGIC - ✅ **Performance**: Otimizações automáticas (compactação, indexação)
# MAGIC
# MAGIC **Quando usar Delta Table?**
# MAGIC - Quando precisa de transações ACID
# MAGIC - Quando precisa de time travel (auditoria, rollback)
# MAGIC - Quando precisa fazer updates/deletes em dados históricos
# MAGIC - Data Warehouses modernos (Lakehouse)
# MAGIC - Pipelines que precisam de garantias de consistência
# MAGIC - Quando múltiplos processos escrevem na mesma tabela

# COMMAND ----------

# Caminho da tabela Delta no Unity Catalog (schema bitcoin_data)
delta_table_path = "pipeline_api_bitcoin.bitcoin_data.bitcoin_data"

# Salvar como Delta Table usando o DataFrame PySpark (modo append se a tabela já existir)
df_spark.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(delta_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Convertendo Delta Table para DataFrame

# COMMAND ----------

# Ler Delta Table como Spark DataFrame
# type: ignore - spark está disponível no ambiente Databricks
df_delta = spark.read.table(delta_table_path)  # noqa: F821

# Mostrar schema
df_delta.printSchema()

# Mostrar dados
df_delta.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consultando Delta Table com SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pipeline_api_bitcoin.bitcoin_data.bitcoin_data
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificando histórico da Delta Table (Time Travel)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY pipeline_api_bitcoin.bitcoin_data.bitcoin_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Resumo do Pipeline
# MAGIC
# MAGIC Este pipeline completo realiza:
# MAGIC
# MAGIC 1. ✅ **Extração**: Busca dados da API Coinbase e cotação USD-BRL
# MAGIC 2. ✅ **Transformação**: Trata dados, converte para BRL e adiciona timestamp
# MAGIC 3. ✅ **Infraestrutura**: Cria Catalog e Schema Lakehouse no Unity Catalog
# MAGIC 4. ✅ **Carga em múltiplos formatos usando Pandas**:
# MAGIC    - **JSON**: Formato texto legível, ideal para dados brutos (salvo com Pandas)
# MAGIC    - **CSV**: Formato texto universal, legível por humanos (salvo com Pandas)
# MAGIC    - **Parquet**: Formato binário columnar, otimizado para Big Data (salvo com Pandas)
# MAGIC 5. ✅ **Conversão Pandas → PySpark**: Converte DataFrame Pandas para PySpark
# MAGIC 6. ✅ **Delta Table**: Salva no Delta Table usando PySpark (formato com ACID transactions e time travel)
# MAGIC 7. ✅ **Conversão**: Delta Table → Spark DataFrame
# MAGIC
# MAGIC **Comparação de Formatos:**
# MAGIC - 📄 **CSV**: Legível, maior, mais lento → Use para debugging e dados pequenos
# MAGIC - 📊 **Parquet**: Binário, menor, mais rápido → Use para Big Data e analytics
# MAGIC - 🎯 **Delta Table**: Parquet + ACID + Time Travel → Use para Data Warehouses e pipelines críticos
# MAGIC
# MAGIC **Próximos passos**: Criar dashboard e agente de IA para análise dos dados!
