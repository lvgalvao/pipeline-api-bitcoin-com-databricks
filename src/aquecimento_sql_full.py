# Databricks notebook source
# MAGIC %md
# MAGIC # 🔥 Aquecimento de SQL — Dashboard Bitcoin
# MAGIC
# MAGIC Antes de qualquer coisa, lembra disso:
# MAGIC
# MAGIC > **SQL é sobre responder perguntas usando dados.**
# MAGIC > 
# MAGIC > Aqui nossas perguntas são:
# MAGIC > * Qual é o **último preço** do Bitcoin?
# MAGIC > * Qual foi o **maior preço** já registrado e **quando**?
# MAGIC > * Qual foi o **menor preço** já registrado e **quando**?
# MAGIC
# MAGIC Vamos responder uma por uma.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📚 O que é SQL?
# MAGIC
# MAGIC **SQL (Structured Query Language)** é a linguagem padrão para trabalhar com bancos de dados relacionais.
# MAGIC
# MAGIC ### 🎯 Conceitos Fundamentais:
# MAGIC
# MAGIC - **SELECT**: O que você quer ver (colunas)
# MAGIC - **FROM**: De onde vêm os dados (tabela)
# MAGIC - **WHERE**: Filtros (condições)
# MAGIC - **ORDER BY**: Ordenação (como organizar)
# MAGIC - **LIMIT**: Limite de linhas (quantas retornar)
# MAGIC - **AS**: Alias (renomear colunas)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1️⃣ LAST PRICE — Último valor coletado
# MAGIC
# MAGIC ### ❓ Pergunta: Qual é o último preço do Bitcoin registrado?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   valor_brl        AS last_price,
# MAGIC   timestamp        AS last_timestamp
# MAGIC FROM pipeline_api_bitcoin.bitcoin_data.bitcoin_data
# MAGIC WHERE criptomoeda = 'BTC'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 O que está acontecendo aqui?
# MAGIC
# MAGIC #### **SELECT**
# MAGIC Diz **quais colunas** queremos ver no resultado.
# MAGIC
# MAGIC ```sql
# MAGIC SELECT valor_brl, timestamp
# MAGIC ```
# MAGIC
# MAGIC #### **AS (Alias)**
# MAGIC Renomeia a coluna para facilitar o uso no dashboard.
# MAGIC
# MAGIC ```sql
# MAGIC valor_brl AS last_price
# MAGIC ```
# MAGIC
# MAGIC Isso é importante para o dashboard entender **o que é o valor principal**.
# MAGIC
# MAGIC #### **FROM**
# MAGIC Define **de onde vêm os dados** (a tabela).
# MAGIC
# MAGIC ```sql
# MAGIC FROM pipeline_api_bitcoin.bitcoin_data.bitcoin_data
# MAGIC ```
# MAGIC
# MAGIC **Estrutura do caminho:**
# MAGIC - `pipeline_api_bitcoin` = Catalog (catálogo)
# MAGIC - `bitcoin_data` = Schema (esquema)
# MAGIC - `bitcoin_data` = Table (tabela)
# MAGIC
# MAGIC #### **WHERE**
# MAGIC Filtra os dados com base em uma condição.
# MAGIC
# MAGIC ```sql
# MAGIC WHERE criptomoeda = 'BTC'
# MAGIC ```
# MAGIC
# MAGIC Sem isso, você misturaria várias moedas (se houvesse outras).
# MAGIC
# MAGIC #### **ORDER BY**
# MAGIC Ordena os resultados.
# MAGIC
# MAGIC ```sql
# MAGIC ORDER BY timestamp DESC
# MAGIC ```
# MAGIC
# MAGIC - `DESC` = Descending (do **mais recente para o mais antigo**)
# MAGIC - `ASC` = Ascending (do mais antigo para o mais recente)
# MAGIC
# MAGIC #### **LIMIT**
# MAGIC Limita o número de linhas retornadas.
# MAGIC
# MAGIC ```sql
# MAGIC LIMIT 1
# MAGIC ```
# MAGIC
# MAGIC Pega **apenas a última linha**, que representa a última coleta.
# MAGIC
# MAGIC ### 📌 Resultado:
# MAGIC
# MAGIC > Último preço do Bitcoin + data/hora da coleta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2️⃣ MAX PRICE — Maior valor já registrado + quando ocorreu
# MAGIC
# MAGIC ### ❓ Pergunta: Qual foi o maior preço já registrado e quando aconteceu?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   valor_brl  AS max_price,
# MAGIC   timestamp  AS max_timestamp
# MAGIC FROM pipeline_api_bitcoin.bitcoin_data.bitcoin_data
# MAGIC WHERE criptomoeda = 'BTC'
# MAGIC ORDER BY valor_brl DESC, timestamp DESC
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 O que muda aqui?
# MAGIC
# MAGIC #### **ORDER BY com múltiplas colunas**
# MAGIC
# MAGIC ```sql
# MAGIC ORDER BY valor_brl DESC, timestamp DESC
# MAGIC ```
# MAGIC
# MAGIC **Como funciona:**
# MAGIC 1. Primeiro ordena por `valor_brl` (do maior para o menor)
# MAGIC 2. Se houver empate no preço, ordena por `timestamp` (do mais recente para o mais antigo)
# MAGIC
# MAGIC **Exemplo prático:**
# MAGIC
# MAGIC | valor_brl | timestamp |
# MAGIC |-----------|-----------|
# MAGIC | 273000.00  | 2025-01-16 10:00 |
# MAGIC | 273000.00  | 2025-01-16 11:00 |
# MAGIC | 267300.00  | 2025-01-16 12:00 |
# MAGIC
# MAGIC Com `ORDER BY valor_brl DESC, timestamp DESC`, a linha com `273000.00` às `11:00` aparece primeiro.
# MAGIC
# MAGIC #### **LIMIT 1**
# MAGIC Retorna **o ponto exato onde o Bitcoin bateu o maior valor**.
# MAGIC
# MAGIC ### 📌 Por que não usar `MAX(valor_usd)`?
# MAGIC
# MAGIC Porque `MAX()` perde o contexto do **timestamp**.
# MAGIC
# MAGIC **Exemplo do problema:**
# MAGIC
# MAGIC ```sql
# MAGIC -- ❌ Isso NÃO funciona como queremos
# MAGIC SELECT 
# MAGIC   MAX(valor_brl) AS max_price,
# MAGIC   timestamp      -- Qual timestamp? O da linha com MAX? Não!
# MAGIC FROM pipeline_api_bitcoin.bitcoin_data.bitcoin_data
# MAGIC ```
# MAGIC
# MAGIC **Solução:**
# MAGIC
# MAGIC ```sql
# MAGIC -- ✅ Isso funciona perfeitamente
# MAGIC SELECT valor_brl, timestamp
# MAGIC FROM ...
# MAGIC ORDER BY valor_brl DESC
# MAGIC LIMIT 1
# MAGIC ```
# MAGIC
# MAGIC Aqui queremos **valor + momento em que aconteceu**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3️⃣ MIN PRICE — Menor valor já registrado + quando ocorreu
# MAGIC
# MAGIC ### ❓ Pergunta: Qual foi o menor preço já registrado e quando aconteceu?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   valor_brl  AS min_price,
# MAGIC   timestamp  AS min_timestamp
# MAGIC FROM pipeline_api_bitcoin.bitcoin_data.bitcoin_data
# MAGIC WHERE criptomoeda = 'BTC'
# MAGIC ORDER BY valor_brl ASC, timestamp DESC
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 O que está sendo feito?
# MAGIC
# MAGIC #### **ORDER BY valor_brl ASC**
# MAGIC Ordena do **menor preço para o maior**.
# MAGIC
# MAGIC ```sql
# MAGIC ORDER BY valor_brl ASC
# MAGIC ```
# MAGIC
# MAGIC - `ASC` = Ascending (crescente: menor → maior)
# MAGIC - `DESC` = Descending (decrescente: maior → menor)
# MAGIC
# MAGIC #### **timestamp DESC (segundo critério)**
# MAGIC Se o mesmo preço aparecer mais de uma vez, pega o **mais recente**.
# MAGIC
# MAGIC O resto segue a mesma lógica do MAX:
# MAGIC - Filtra BTC
# MAGIC - Pega o momento exato
# MAGIC - Retorna uma única linha
# MAGIC
# MAGIC ### 📌 Resultado:
# MAGIC
# MAGIC > Menor preço histórico do Bitcoin + data/hora.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧩 Conceitos Importantes Desse Aquecimento
# MAGIC
# MAGIC ### 🔹 `ORDER BY + LIMIT`
# MAGIC
# MAGIC Esse combo é **poderosíssimo**:
# MAGIC
# MAGIC - ✅ Último registro
# MAGIC - ✅ Maior valor
# MAGIC - ✅ Menor valor
# MAGIC - ✅ Primeira ocorrência
# MAGIC - ✅ Ranking simples
# MAGIC
# MAGIC **Exemplos:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Último registro
# MAGIC ORDER BY timestamp DESC LIMIT 1
# MAGIC
# MAGIC -- Maior valor
# MAGIC ORDER BY valor_brl DESC LIMIT 1
# MAGIC
# MAGIC -- Menor valor
# MAGIC ORDER BY valor_brl ASC LIMIT 1
# MAGIC
# MAGIC -- Top 5 maiores preços
# MAGIC ORDER BY valor_brl DESC LIMIT 5
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔹 `AS` (Alias)
# MAGIC
# MAGIC Não é só estética:
# MAGIC
# MAGIC - ✅ Deixa o SQL mais legível
# MAGIC - ✅ Facilita uso no dashboard
# MAGIC - ✅ Evita nomes confusos como `max(valor_usd)`
# MAGIC
# MAGIC **Exemplo:**
# MAGIC
# MAGIC ```sql
# MAGIC -- Sem alias (confuso)
# MAGIC SELECT valor_brl FROM ...
# MAGIC
# MAGIC -- Com alias (claro)
# MAGIC SELECT valor_brl AS last_price FROM ...
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔹 SQL para Análise ≠ SQL para Engenharia
# MAGIC
# MAGIC Aqui estamos usando SQL **analítico**:
# MAGIC
# MAGIC - ✅ Leitura (`SELECT`)
# MAGIC - ✅ Filtro (`WHERE`)
# MAGIC - ✅ Ordenação (`ORDER BY`)
# MAGIC - ✅ Contexto de negócio
# MAGIC
# MAGIC Nada de `INSERT`, `UPDATE`, `DELETE`.
# MAGIC
# MAGIC **Diferença:**
# MAGIC
# MAGIC | Tipo | Operações | Uso |
# MAGIC |------|-----------|-----|
# MAGIC | **Analítico** | `SELECT`, `WHERE`, `ORDER BY` | Dashboards, relatórios, análises |
# MAGIC | **Engenharia** | `INSERT`, `UPDATE`, `DELETE` | Pipelines, transformações, carga |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧠 Regra de Ouro (Guarda Isso)
# MAGIC
# MAGIC > **Se você precisa do valor + quando aconteceu,
# MAGIC > não use agregação cega (`MAX`, `MIN`).
# MAGIC > Ordene e limite.**
# MAGIC
# MAGIC ### ❌ Errado:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   MAX(valor_brl) AS max_price,
# MAGIC   timestamp  -- Qual timestamp? Não funciona!
# MAGIC FROM pipeline_api_bitcoin.bitcoin_data.bitcoin_data
# MAGIC ```
# MAGIC
# MAGIC ### ✅ Correto:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT 
# MAGIC   valor_brl AS max_price,
# MAGIC   timestamp AS max_timestamp
# MAGIC FROM pipeline_api_bitcoin.bitcoin_data.bitcoin_data
# MAGIC ORDER BY valor_brl DESC
# MAGIC LIMIT 1
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Resumo dos 3 Queries do Dashboard
# MAGIC
# MAGIC | Query | Ordenação | Resultado |
# MAGIC |-------|-----------|-----------|
# MAGIC | **LAST PRICE** | `ORDER BY timestamp DESC` | Último preço em BRL registrado |
# MAGIC | **MAX PRICE** | `ORDER BY valor_brl DESC` | Maior preço em BRL + quando |
# MAGIC | **MIN PRICE** | `ORDER BY valor_brl ASC` | Menor preço em BRL + quando |
# MAGIC
# MAGIC **Todos usam:**
# MAGIC - `WHERE criptomoeda = 'BTC'` (filtro)
# MAGIC - `LIMIT 1` (apenas 1 resultado)
# MAGIC - `AS` (alias para dashboard)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Próximos Passos
# MAGIC
# MAGIC Agora que você domina esses conceitos básicos, pode explorar:
# MAGIC
# MAGIC - 📈 **Variação percentual** entre o último e o anterior
# MAGIC - 📊 **Média móvel simples** (últimos N registros)
# MAGIC - 🔢 **Window Functions** (`ROW_NUMBER`, `RANK`, `LAG`, `LEAD`)
# MAGIC - 📉 **Agregações** (`COUNT`, `SUM`, `AVG`, `GROUP BY`)
# MAGIC - 🔗 **Joins** (combinar múltiplas tabelas)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Pronto para criar seu dashboard! 🚀**

