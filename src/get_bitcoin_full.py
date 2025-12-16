# Databricks notebook source
# MAGIC %md
# MAGIC # 💰 Extração de Dados Bitcoin - API Coinbase
# MAGIC
# MAGIC Este notebook demonstra como:
# MAGIC - Extrair dados da API da Coinbase
# MAGIC - Adicionar timestamp aos dados
# MAGIC - Salvar dados em JSON, CSV e Parquet
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
import json
from datetime import datetime
import pandas as pd
import os

# Para trabalhar com Spark e Delta Tables
from pyspark.sql import SparkSession

print("✅ Bibliotecas importadas com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configurando URLs e Parâmetros da API

# COMMAND ----------

# URL da API Coinbase para obter o preço spot
url = 'https://api.coinbase.com/v2/prices/spot'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Extraindo Dados da API Coinbase

# COMMAND ----------

def extrair_dados_bitcoin():
    """Extrai o JSON completo da API da Coinbase."""
    url = 'https://api.coinbase.com/v2/prices/spot'
    resposta = requests.get(url)
    return resposta.json()

# Extraindo dados
dados_json = extrair_dados_bitcoin()
print("✅ Dados extraídos com sucesso!")
print("\nResposta da API:")
print(json.dumps(dados_json, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Extraindo Cotação USD-BRL da API de Economia

# COMMAND ----------

def extrair_cotacao_usd_brl():
    """Extrai a cotação USD-BRL da API AwesomeAPI."""
    url = 'https://economia.awesomeapi.com.br/json/last/USD-BRL'
    resposta = requests.get(url)
    return resposta.json()

# Extraindo cotação USD-BRL
cotacao_json = extrair_cotacao_usd_brl()
print("✅ Cotação USD-BRL extraída com sucesso!")
print("\nResposta da API de Economia:")
print(json.dumps(cotacao_json, indent=2))

# Extraindo a taxa de conversão (bid = taxa de compra)
taxa_usd_brl = float(cotacao_json['USDBRL']['bid'])
print(f"\n💱 Taxa de conversão USD-BRL: R$ {taxa_usd_brl:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Tratando Dados, Adicionando Timestamp e Convertendo para BRL

# COMMAND ----------

def tratar_dados_bitcoin(dados_json, taxa_usd_brl):
    """Transforma os dados brutos da API, renomeia colunas, adiciona timestamp e converte para BRL."""
    valor_usd = float(dados_json['data']['amount'])
    criptomoeda = dados_json['data']['base']
    moeda_original = dados_json['data']['currency']
    
    # Convertendo de USD para BRL
    valor_brl = valor_usd * taxa_usd_brl
    
    # Adicionando timestamp
    timestamp = datetime.now().isoformat()
    timestamp_readable = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    dados_tratados = [{
        "valor_usd": valor_usd,
        "valor_brl": valor_brl,
        "criptomoeda": criptomoeda,
        "moeda_original": moeda_original,
        "taxa_conversao_usd_brl": taxa_usd_brl,
        "timestamp": timestamp,
        "timestamp_readable": timestamp_readable
    }]
    
    return dados_tratados

# Tratando os dados e convertendo para BRL
dados_bitcoin = tratar_dados_bitcoin(dados_json, taxa_usd_brl)
print("✅ Dados tratados e convertidos para BRL com sucesso!")
print("\nDados processados:")
print(json.dumps(dados_bitcoin, indent=2, ensure_ascii=False))
print(f"\n💰 Bitcoin em USD: ${dados_bitcoin[0]['valor_usd']:,.2f}")
print(f"💰 Bitcoin em BRL: R$ {dados_bitcoin[0]['valor_brl']:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Configurando nosso Catálogo
# MAGIC
# MAGIC ### 📁 O que é um Catálogo no Databricks?
# MAGIC
# MAGIC **Catálogos** são o nível mais alto de organização de dados no Databricks, dentro do **Unity Catalog**. Eles funcionam como um *container lógico* que agrupa schemas, tabelas, views e volumes, garantindo governança, segurança e organização em escala.
# MAGIC
# MAGIC Um Catálogo permite:
# MAGIC - ✅ **Governança centralizada**: Controle de acesso unificado para dados, arquivos e assets
# MAGIC - ✅ **Organização lógica**: Separação clara por domínio, time ou finalidade (ex: `main`, `dev`, `analytics`)
# MAGIC - ✅ **Segurança**: Permissões granulares por catálogo, schema e objeto
# MAGIC - ✅ **Padronização**: Base para boas práticas de arquitetura de dados
# MAGIC
# MAGIC ### 🧱 Hierarquia no Databricks
# MAGIC
# MAGIC A organização segue a hierarquia:
# MAGIC
# MAGIC `Catalog → Schema → Tabelas / Views / Volumes`

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS pipeline_api_bitcoin
# MAGIC COMMENT 'Catálogo de demonstração criado para o workshop de pipeline_api_bitcoin';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Criar um SCHEMA (database) no catálogo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS pipeline_api_bitcoin.datalake
# MAGIC COMMENT 'Schema Datalake para salvar dados brutos e heterogêneos';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Criar um Volume no catálogo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS pipeline_api_bitcoin.datalake.raw_files
# MAGIC COMMENT 'Volume para arquivos brutos de ingestão inicial';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Salvando em JSON

# COMMAND ----------

# Pega o timestamp do próprio evento
event_ts = dados_bitcoin[0]["timestamp"]

# Converte para formato seguro para nome de arquivo
ts = datetime.fromisoformat(event_ts).strftime("%Y%m%d_%H%M%S_%f")

path = (
    f"/Volumes/pipeline_api_bitcoin/datalake/raw_files/"
    f"bitcoin_{ts}.json"
)

with open(path, "w", encoding='utf-8') as f:
    json.dump(dados_bitcoin, f, indent=2, ensure_ascii=False)

print(f"✅ JSON salvo em: {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Salvando como CSV
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

# Criar DataFrame do Pandas
df_csv = pd.DataFrame(dados_bitcoin)

# Caminho do arquivo CSV no volume
csv_path = f"/Volumes/pipeline_api_bitcoin/datalake/raw_files/bitcoin_{ts}.csv"

# Salvar como CSV
df_csv.to_csv(csv_path, index=False, encoding='utf-8')

# Verificar tamanho do arquivo
tamanho_csv = os.path.getsize(csv_path)

print(f"✅ CSV salvo em: {csv_path}")
print(f"📦 Tamanho: {tamanho_csv:,} bytes ({tamanho_csv/1024:.2f} KB)")
print(f"📊 Total de registros: {len(df_csv)}")

# Mostrar dados
print("\n📄 Conteúdo do CSV:")
print(df_csv)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Salvando como Parquet
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

# Criar DataFrame do Pandas
df_parquet = pd.DataFrame(dados_bitcoin)

# Caminho do arquivo Parquet no volume
parquet_path = f"/Volumes/pipeline_api_bitcoin/datalake/raw_files/bitcoin_{ts}.parquet"

# Salvar como Parquet
df_parquet.to_parquet(parquet_path, index=False, engine='pyarrow')

# Verificar tamanho do arquivo
tamanho_parquet = os.path.getsize(parquet_path)

print(f"✅ Parquet salvo em: {parquet_path}")
print(f"📦 Tamanho: {tamanho_parquet:,} bytes ({tamanho_parquet/1024:.2f} KB)")
print(f"📊 Total de registros: {len(df_parquet)}")

# Mostrar dados
print("\n📊 Conteúdo do Parquet:")
print(df_parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Comparação: CSV vs Parquet

# COMMAND ----------

if os.path.exists(csv_path) and os.path.exists(parquet_path):
    tamanho_csv = os.path.getsize(csv_path)
    tamanho_parquet = os.path.getsize(parquet_path)
    
    print("=" * 70)
    print("COMPARAÇÃO: CSV vs PARQUET")
    print("=" * 70)
    
    print("\n📄 CSV:")
    print(f"   Tamanho: {tamanho_csv:,} bytes ({tamanho_csv/1024:.2f} KB)")
    print("   Tipo: Texto (Row-based)")
    print("   Legível: Sim (Excel, Bloco de Notas)")
    
    print("\n📊 Parquet:")
    print(f"   Tamanho: {tamanho_parquet:,} bytes ({tamanho_parquet/1024:.2f} KB)")
    print("   Tipo: Binário (Columnar)")
    print("   Legível: Não (requer ferramentas especiais)")
    
    if tamanho_csv > 0:
        economia = ((tamanho_csv - tamanho_parquet) / tamanho_csv) * 100
        print(f"\n💾 Economia com Parquet: {economia:.1f}%")
        print(f"   (CSV é {tamanho_csv/tamanho_parquet:.1f}x maior que Parquet)")
    
    print("\n" + "=" * 70)
    print("RESUMO")
    print("=" * 70)
    print("\n📄 CSV: Legível, maior, mais lento → Use para dados pequenos e debugging")
    print("📊 Parquet: Binário, menor, mais rápido → Use para Big Data e analytics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Salvando como Delta Table
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

# Criar SparkSession
spark = SparkSession.builder.appName("BitcoinPipeline").getOrCreate()

# Converter DataFrame do Pandas para Spark DataFrame
df_spark = spark.createDataFrame(df_parquet)

# Mostrar schema
print("📊 Schema do DataFrame Spark:")
df_spark.printSchema()

# Mostrar dados
print("\n📊 Dados do DataFrame Spark:")
df_spark.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salvando como Delta Table no Unity Catalog

# COMMAND ----------

# Caminho da tabela Delta no Unity Catalog
delta_table_path = "pipeline_api_bitcoin.datalake.bitcoin_data"

# Salvar como Delta Table (modo append se a tabela já existir)
df_spark.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable(delta_table_path)

print(f"✅ Delta Table salva em: {delta_table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Convertendo Delta Table para DataFrame
# MAGIC
# MAGIC Agora vamos ler a Delta Table e convertê-la para DataFrame para análise.

# COMMAND ----------

# Ler Delta Table como Spark DataFrame
df_delta = spark.read.table(delta_table_path)

print("✅ Delta Table lida com sucesso!")
print(f"📊 Total de registros: {df_delta.count()}")

# Mostrar schema
print("\n📊 Schema da Delta Table:")
df_delta.printSchema()

# Mostrar dados
print("\n📊 Dados da Delta Table:")
df_delta.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convertendo Spark DataFrame para Pandas DataFrame

# COMMAND ----------

# Converter Spark DataFrame para Pandas DataFrame
df_pandas = df_delta.toPandas()

print("✅ Spark DataFrame convertido para Pandas DataFrame!")
print(f"\n📊 Tipo: {type(df_pandas)}")
print(f"📊 Shape: {df_pandas.shape}")
print("\n📊 Dados:")
print(df_pandas)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consultando Delta Table com SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM pipeline_api_bitcoin.datalake.bitcoin_data
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificando histórico da Delta Table (Time Travel)

# MAGIC Uma das grandes vantagens do Delta Lake é o **Time Travel** - podemos acessar versões anteriores dos dados!

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY pipeline_api_bitcoin.datalake.bitcoin_data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 15. Resumo do Pipeline
# MAGIC
# MAGIC Este pipeline completo realiza:
# MAGIC
# MAGIC 1. ✅ **Extração**: Busca dados da API Coinbase
# MAGIC 2. ✅ **Transformação**: Trata dados e adiciona timestamp
# MAGIC 3. ✅ **Infraestrutura**: Cria Catalog, Schema e Volume no Unity Catalog
# MAGIC 4. ✅ **Carga em múltiplos formatos**:
# MAGIC    - **JSON**: Formato texto legível, ideal para dados brutos
# MAGIC    - **CSV**: Formato texto universal, legível por humanos
# MAGIC    - **Parquet**: Formato binário columnar, otimizado para Big Data
# MAGIC    - **Delta Table**: Formato com ACID transactions e time travel
# MAGIC 5. ✅ **Conversão**: Delta Table → Spark DataFrame → Pandas DataFrame
# MAGIC
# MAGIC **Comparação de Formatos:**
# MAGIC - 📄 **CSV**: Legível, maior, mais lento → Use para debugging e dados pequenos
# MAGIC - 📊 **Parquet**: Binário, menor, mais rápido → Use para Big Data e analytics
# MAGIC - 🎯 **Delta Table**: Parquet + ACID + Time Travel → Use para Data Warehouses e pipelines críticos
# MAGIC
# MAGIC **Próximos passos**: Criar dashboard e agente de IA para análise dos dados!

# COMMAND ----------
