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

# TODO: Importar as bibliotecas necessárias
# - requests (para fazer requisições HTTP)
# - json (para trabalhar com JSON)
# - datetime (para trabalhar com timestamps)

# Digite o código aqui:


# No Databricks, o Spark já está disponível como 'spark', não precisa criar SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Extraindo e Transformando Dados

# COMMAND ----------

# TODO: Criar função para extrair dados do Bitcoin da API Coinbase
# URL: https://api.coinbase.com/v2/prices/spot
# Retornar: resposta.json()

def extrair_dados_bitcoin():
    """Extrai o JSON completo da API da Coinbase."""
    # Digite o código aqui:
    pass

# COMMAND ----------

# TODO: Criar função para extrair cotação USD-BRL da API CurrencyFreaks
# URL: https://api.currencyfreaks.com/v2.0/rates/latest?apikey={api_key}
# API Key: 'bf8ddcbf744d4e7897e836941c32e39f'
# Retornar: resposta.json()

def extrair_cotacao_usd_brl():
    """Extrai a cotação USD-BRL da API CurrencyFreaks."""
    # Digite o código aqui:
    pass

# COMMAND ----------

# TODO: Criar função para tratar os dados do Bitcoin
# Parâmetros: dados_json, taxa_usd_brl
# Extrair: valor_usd, criptomoeda, moeda_original
# Calcular: valor_brl = valor_usd * taxa_usd_brl
# Adicionar: timestamp usando datetime.now()
# Retornar: lista com dicionário contendo os dados tratados

def tratar_dados_bitcoin(dados_json, taxa_usd_brl):
    """Transforma os dados brutos da API, renomeia colunas, adiciona timestamp e converte para BRL."""
    # Digite o código aqui:
    pass

# COMMAND ----------

# TODO: Executar as funções para extrair e transformar os dados
# 1. Chamar extrair_dados_bitcoin() e armazenar em Dados_bitcoin
# 2. Chamar extrair_cotacao_usd_brl() e armazenar em dados_cotacao
# 3. Extrair a taxa de conversão: dados_cotacao['rates']['BRL']
# 4. Chamar tratar_dados_bitcoin() com os dados e a taxa

# Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configurando Unity Catalog

# COMMAND ----------

# TODO: Criar o Catalog no Unity Catalog
# Nome: pipeline_api_bitcoin
# Comentário: 'Catálogo de demonstração criado para o workshop de pipeline_api_bitcoin'

# MAGIC %sql
# MAGIC -- Digite o SQL aqui:


# COMMAND ----------

# TODO: Criar o Schema lakehouse
# Nome: pipeline_api_bitcoin.lakehouse
# Comentário: 'Schema Lakehouse para salvar dados processados'

# MAGIC %sql
# MAGIC -- Digite o SQL aqui:


# COMMAND ----------

# TODO: Criar o Volume para arquivos brutos
# Nome: pipeline_api_bitcoin.lakehouse.raw_files
# Comentário: 'Volume para arquivos brutos de ingestão inicial'

# MAGIC %sql
# MAGIC -- Digite o SQL aqui:


# COMMAND ----------

# TODO: Criar o Schema bitcoin_data
# Nome: pipeline_api_bitcoin.bitcoin_data
# Comentário: 'Schema para armazenar dados de Bitcoin processados'

# MAGIC %sql
# MAGIC -- Digite o SQL aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criando Spark DataFrame
# MAGIC
# MAGIC No Databricks, o objeto `spark` (SparkSession) já está disponível automaticamente.

# COMMAND ----------

# TODO: Criar Spark DataFrame a partir dos dados tratados
# Usar: spark.createDataFrame(dados_bitcoin_tratado)
# Mostrar o schema usando: df.printSchema()

# Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Salvando em JSON usando PySpark

# COMMAND ----------

# TODO: Salvar DataFrame como JSON
# 1. Pegar o timestamp do primeiro registro: dados_bitcoin_tratado[0]["timestamp"]
# 2. Converter para formato de arquivo: .strftime("%Y%m%d_%H%M%S_%f")
# 3. Criar caminho: f"/Volumes/pipeline_api_bitcoin/lakehouse/raw_files/bitcoin_{ts}.json"
# 4. Salvar usando: df.write.mode("overwrite").json(json_path)

# Digite o código aqui:


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

# TODO: Salvar DataFrame como CSV
# 1. Criar caminho: f"/Volumes/pipeline_api_bitcoin/lakehouse/raw_files/bitcoin_{ts}.csv"
# 2. Salvar usando: df.write.mode("overwrite").option("header", "true").csv(csv_path)

# Digite o código aqui:


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

# TODO: Salvar DataFrame como Parquet
# 1. Criar caminho: f"/Volumes/pipeline_api_bitcoin/lakehouse/raw_files/bitcoin_{ts}.parquet"
# 2. Salvar usando: df.write.mode("overwrite").parquet(parquet_path)

# Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Salvando como Delta Table
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

# TODO: Salvar DataFrame como Delta Table
# 1. Definir caminho: "pipeline_api_bitcoin.bitcoin_data.bitcoin_data"
# 2. Salvar usando: df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(delta_table_path)

# Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Convertendo Delta Table para DataFrame

# COMMAND ----------

# TODO: Ler a Delta Table como Spark DataFrame
# Usar: spark.read.table(delta_table_path)
# Mostrar schema: df_delta.printSchema()
# Mostrar dados: df_delta.show(truncate=False)

# Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Consultando Delta Table com SQL

# COMMAND ----------

# TODO: Consultar a Delta Table usando SQL
# Selecionar todas as colunas, ordenar por timestamp DESC, limitar a 10 registros

# MAGIC %sql
# MAGIC -- Digite o SQL aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Verificando histórico da Delta Table (Time Travel)

# COMMAND ----------

# TODO: Verificar o histórico da Delta Table
# Usar: DESCRIBE HISTORY

# MAGIC %sql
# MAGIC -- Digite o SQL aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Resumo do Pipeline
# MAGIC
# MAGIC Este pipeline completo realiza:
# MAGIC
# MAGIC 1. ✅ **Extração**: Busca dados da API Coinbase e cotação USD-BRL
# MAGIC 2. ✅ **Transformação**: Trata dados, converte para BRL e adiciona timestamp
# MAGIC 3. ✅ **Infraestrutura**: Cria Catalog e Schema Lakehouse no Unity Catalog
# MAGIC 4. ✅ **Carga em múltiplos formatos usando PySpark**:
# MAGIC    - **JSON**: Formato texto legível, ideal para dados brutos
# MAGIC    - **CSV**: Formato texto universal, legível por humanos (salvo com PySpark)
# MAGIC    - **Parquet**: Formato binário columnar, otimizado para Big Data (salvo com PySpark)
# MAGIC    - **Delta Table**: Formato com ACID transactions e time travel
# MAGIC 5. ✅ **Conversão**: Delta Table → Spark DataFrame
# MAGIC
# MAGIC **Comparação de Formatos:**
# MAGIC - 📄 **CSV**: Legível, maior, mais lento → Use para debugging e dados pequenos
# MAGIC - 📊 **Parquet**: Binário, menor, mais rápido → Use para Big Data e analytics
# MAGIC - 🎯 **Delta Table**: Parquet + ACID + Time Travel → Use para Data Warehouses e pipelines críticos
# MAGIC
# MAGIC **Próximos passos**: Criar dashboard e agente de IA para análise dos dados!

