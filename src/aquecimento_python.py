# Databricks notebook source
# MAGIC %md
# MAGIC # Aquecimento: Fundamentos de Python
# MAGIC
# MAGIC Bem-vindo ao aquecimento! Este notebook vai revisar os conceitos fundamentais de Python que você precisa conhecer antes de começar o projeto de ETL.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖨️ 1. Print
# MAGIC
# MAGIC A função `print()` é uma das mais importantes em Python. Ela exibe informações na tela (ou no output do notebook).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print Básico - Hello World!

# COMMAND ----------

# Print simples
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Print com Variáveis

# COMMAND ----------

# Print com variáveis
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ### Print com Formatação (f-strings)
# MAGIC
# MAGIC **f-strings** são a forma moderna e recomendada de formatar strings em Python!

# COMMAND ----------

# Usando f-strings (recomendado!)
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 2. Variáveis
# MAGIC
# MAGIC Variáveis são como "caixas" onde guardamos informações. Em Python, você não precisa declarar o tipo da variável - o Python descobre automaticamente!

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Tipos de Variáveis Simples
# MAGIC
# MAGIC Python tem três tipos de dados básicos e simples que você vai usar constantemente:

# COMMAND ----------

# MAGIC %md
# MAGIC #### String (str) - Texto
# MAGIC
# MAGIC Strings são usadas para armazenar texto. Podem ser criadas com aspas simples ou duplas.

# COMMAND ----------

# Exemplos de strings
# TODO: Digite o código aqui:


# Verificando o tipo
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC #### Int (int) - Números Inteiros
# MAGIC
# MAGIC Inteiros são números sem parte decimal. Usados para contagens, índices, quantidades.

# COMMAND ----------

# Exemplos de inteiros
# TODO: Digite o código aqui:


# Verificando o tipo
# TODO: Digite o código aqui:

# Operações com inteiros
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC #### Float (float) - Números Decimais
# MAGIC
# MAGIC Floats são números com parte decimal. Essenciais para valores monetários, preços, percentuais.

# COMMAND ----------

# Exemplos de floats
# TODO: Digite o código aqui:


# Verificando o tipo
# TODO: Digite o código aqui:

# Operações com floats
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Resumo dos Tipos Simples

# COMMAND ----------

# Criando variáveis de cada tipo
# TODO: Digite o código aqui:


# Verificando todos os tipos
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Variáveis com Dados da API
# MAGIC
# MAGIC Vamos simular dados que viriam da API da Coinbase usando os tipos simples:

# COMMAND ----------

# Simulando dados da API usando tipos simples
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 3. Dicionários (dict) - Pares Chave-Valor
# MAGIC
# MAGIC Dicionários armazenam dados em pares chave-valor. São muito úteis para representar dados estruturados, como respostas de APIs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Criando Dicionários

# COMMAND ----------

# Criando dicionários
# TODO: Digite o código aqui:


# Verificando o tipo
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Acessando Valores de um Dicionário
# MAGIC
# MAGIC Você pode acessar valores usando a chave entre colchetes ou o método `.get()`.

# COMMAND ----------

# Acessando valores
# TODO: Digite o código aqui:


# Usando get() (mais seguro - retorna None se a chave não existir)
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Dicionários Aninhados
# MAGIC
# MAGIC Dicionários podem conter outros dicionários, o que é muito comum em respostas de APIs.

# COMMAND ----------

# Simulando resposta completa da API Coinbase
# TODO: Digite o código aqui:


# Acessando valores aninhados
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 4. Métodos Úteis para ETL
# MAGIC
# MAGIC Métodos são funções que pertencem a objetos. Vamos focar nos métodos mais úteis para trabalhar com dados de APIs.

# COMMAND ----------

# Simulando dados brutos da API
# TODO: Digite o código aqui:


# Extrair e transformar dados
# TODO: Digite o código aqui:


# Converter string para float
# TODO: Digite o código aqui:


# Criar novo dicionário formatado
# TODO: Digite o código aqui:


# Exibir dados transformados
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🎯 Exercício Prático
# MAGIC
# MAGIC Vamos praticar tudo que aprendemos!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercício: Processar Dados da API
# MAGIC
# MAGIC Complete o código abaixo para processar os dados simulados da API:

# COMMAND ----------

# Dados simulados da API Coinbase
dados_api = {
    "data": {
        "base": "BTC",
        "currency": "USD",
        "amount": "45230.75"
    },
    "timestamp": "2025-12-16T19:30:00Z"
}

# TODO: Extraia o preço e converta para float
# TODO: Digite o código aqui:


# TODO: Crie uma string formatada com o par de moedas
# TODO: Digite o código aqui:


# TODO: Use print com f-string para exibir os dados
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 5. Funções
# MAGIC
# MAGIC Funções são blocos de código reutilizáveis que executam uma tarefa específica. Elas ajudam a organizar o código e evitar repetição.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Criando uma Função de Soma

# COMMAND ----------

# Criando uma função para somar dois números
# TODO: Digite o código aqui:


# Usando a função
# TODO: Digite o código aqui:


# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## ✅ Resumo
# MAGIC
# MAGIC Neste aquecimento, você aprendeu:
# MAGIC
# MAGIC 1. **Print**: Como exibir informações na tela (incluindo o famoso "Hello World!")
# MAGIC 2. **Variáveis**: Como armazenar dados em Python (str, int, float)
# MAGIC 3. **Dicionários**: Como trabalhar com dados estruturados de APIs
# MAGIC 4. **Métodos Úteis para ETL**: Como transformar dados brutos em dados formatados
# MAGIC 5. **Funções**: Como criar blocos de código reutilizáveis
# MAGIC
# MAGIC Esses são os fundamentos que você vai usar durante todo o projeto de ETL!
# MAGIC
# MAGIC 🚀 **Pronto para começar o pipeline? Vamos lá!**
