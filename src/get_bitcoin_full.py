# Databricks notebook source
# MAGIC %md
# MAGIC # 💰 Extração de Dados Bitcoin - API Coinbase
# MAGIC
# MAGIC Este notebook demonstra como:
# MAGIC - Extrair dados da API da Coinbase
# MAGIC - Adicionar timestamp aos dados
# MAGIC - Atualizar e salvar dados em JSON
# MAGIC - Salvar dados em formato **Parquet** (otimizado para Big Data)
# MAGIC - Salvar dados em formato **CSV** (legível por humanos)
# MAGIC - Trabalhar com **Volumes** no Databricks
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Importando Bibliotecas Necessárias

# COMMAND ----------

import requests

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Tratando Dados e Adicionando Timestamp

# COMMAND ----------

def tratar_dados_bitcoin(dados_json):
    """Transforma os dados brutos da API, renomeia colunas e adiciona timestamp."""
    valor = dados_json['data']['amount']
    criptomoeda = dados_json['data']['base']
    moeda = dados_json['data']['currency']
    
    # Adicionando timestamp (importante para rastrear quando o dado foi coletado)
    timestamp = datetime.now().isoformat()
    
    dados_tratados = [{
        "valor": valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda,
        "timestamp": timestamp,
    }]
    
    return dados_tratados

# Tratando os dados
dados_bitcoin = tratar_dados_bitcoin(dados_json)


# COMMAND ----------

print(dados_bitcoin)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Configurando nosso Catálogo
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

# MAGIC
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS pipeline_api_bitcoin
# MAGIC COMMENT 'Catálogo de demonstração criado para o workshop de pipeline_api_bitcoin';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Criar um SCHEMA (database) no catálogo
# MAGIC

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS pipeline_api_bitcoin.datalake
# MAGIC COMMENT 'Schema Datalake para salvar dados brutos e heterogêneos';
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Criar um Volume no catálogo
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME pipeline_api_bitcoin.datalake.raw_files
# MAGIC COMMENT 'Volume para arquivos brutos de ingestão inicial';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Salvando e Atualizando JSON
# MAGIC
# MAGIC JSON é um formato de texto legível por humanos, ideal para armazenar dados estruturados de forma simples.

# COMMAND ----------

import json
from datetime import datetime



# COMMAND ----------

# pega o timestamp do próprio evento
event_ts = dados_bitcoin[0]["timestamp"]

# converte para formato seguro para nome de arquivo
ts = datetime.fromisoformat(event_ts).strftime("%Y%m%d_%H%M%S_%f")

path = (
    f"/Volumes/pipeline_api_bitcoin/datalake/raw_files/"
    f"bitcoin_{ts}.json"
)

with open(path, "w") as f:
    json.dump(dados_bitcoin, f)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Convertendo para DataFrame e Salvando como Parquet
# MAGIC
# MAGIC ### 📊 O que é Parquet?
# MAGIC
# MAGIC **Parquet** é um formato de arquivo **binário e columnar** otimizado para Big Data:
# MAGIC
# MAGIC **Características:**
# MAGIC - ✅ **Compressão**: Arquivos muito menores que CSV (até 90% de economia)
# MAGIC - ✅ **Performance**: Leitura rápida, especialmente para consultas analíticas
# MAGIC - ✅ **Big Data**: Ideal para processar grandes volumes de dados (terabytes/petabytes)
# MAGIC - ✅ **Schema**: Mantém informações sobre tipos de dados automaticamente
# MAGIC - ✅ **Columnar**: Armazena dados por coluna, não por linha (otimizado para analytics)
# MAGIC - ❌ **Não é legível por humanos**: Precisa de ferramentas especiais para ler (Pandas, Spark, etc.)
# MAGIC
# MAGIC **Quando usar Parquet?**
# MAGIC - Processamento de grandes volumes de dados
# MAGIC - Data Lakes e Data Warehouses
# MAGIC - Quando performance e economia de espaço são importantes
# MAGIC - Análises analíticas (OLAP)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Salvando como CSV
# MAGIC
# MAGIC ### 📄 O que é CSV?
# MAGIC
# MAGIC **CSV (Comma-Separated Values)** é um formato de arquivo **texto** simples e universal:
# MAGIC
# MAGIC **Características:**
# MAGIC - ✅ **Legível por humanos**: Pode ser aberto no Excel, Bloco de Notas, Google Sheets, etc.
# MAGIC - ✅ **Simples**: Fácil de entender e debugar
# MAGIC - ✅ **Universal**: Suportado por praticamente todas as ferramentas
# MAGIC - ✅ **Row-based**: Armazena dados por linha (otimizado para transações)
# MAGIC - ❌ **Maior tamanho**: Arquivos maiores que Parquet (sem compressão)
# MAGIC - ❌ **Mais lento**: Leitura e escrita mais lentas em grandes volumes
# MAGIC - ❌ **Sem schema**: Não preserva tipos de dados automaticamente (tudo é texto)
# MAGIC
# MAGIC **Quando usar CSV?**
# MAGIC - Dados pequenos ou médios
# MAGIC - Quando precisa ser legível por humanos
# MAGIC - Integração com ferramentas que não suportam Parquet
# MAGIC - Debugging e inspeção manual dos dados
# MAGIC - Exportação para Excel ou outras ferramentas de negócio

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Comparação Detalhada: CSV vs Parquet
# MAGIC
# MAGIC Vamos comparar os tamanhos dos arquivos e entender as diferenças práticas entre os dois formatos:

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Resumo do Pipeline
# MAGIC
# MAGIC Este pipeline completo realiza:
# MAGIC
# MAGIC 1. ✅ **Extração**: Busca dados da API Coinbase
# MAGIC 2. ✅ **Transformação**: Trata e renomeia colunas, adiciona timestamp
# MAGIC 3. ✅ **Carga**: Salva em múltiplos formatos:
# MAGIC    - JSON (atualização incremental)
# MAGIC    - Parquet (otimizado para Big Data)
# MAGIC    - CSV (legível por humanos)
# MAGIC 4. ✅ **Armazenamento**: Utiliza Volumes do Databricks para organização
# MAGIC
# MAGIC **Próximos passos**: Criar dashboard e agente de IA para análise dos dados!
