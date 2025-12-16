# Databricks notebook source
# MAGIC %md
# MAGIC # 🔥 Aquecimento: Fundamentos de Python
# MAGIC 
# MAGIC Bem-vindo ao aquecimento! Este notebook vai revisar os conceitos fundamentais de Python que você precisa conhecer antes de começar o projeto de ETL.
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📌 1. Variáveis
# MAGIC 
# MAGIC Variáveis são como "caixas" onde guardamos informações. Em Python, você não precisa declarar o tipo da variável - o Python descobre automaticamente!

# COMMAND ----------

# Exemplos de variáveis
nome = "Bitcoin"
preco = 45000.50
quantidade = 10
ativo = True

print(f"Moeda: {nome}")
print(f"Preço: ${preco}")
print(f"Quantidade: {quantidade}")
print(f"Ativo: {ativo}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tipos de Variáveis
# MAGIC 
# MAGIC Python tem vários tipos de dados:
# MAGIC - **str** (string): texto
# MAGIC - **int** (inteiro): números inteiros
# MAGIC - **float** (ponto flutuante): números decimais
# MAGIC - **bool** (booleano): True ou False
# MAGIC - **list** (lista): coleção ordenada
# MAGIC - **dict** (dicionário): pares chave-valor

# COMMAND ----------

# Verificando os tipos das variáveis
print(f"Tipo de 'nome': {type(nome)}")
print(f"Tipo de 'preco': {type(preco)}")
print(f"Tipo de 'quantidade': {type(quantidade)}")
print(f"Tipo de 'ativo': {type(ativo)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variáveis com Dados da API
# MAGIC 
# MAGIC Vamos simular dados que viriam da API da Coinbase:

# COMMAND ----------

# Simulando dados da API
moeda = "BTC-USD"
preco_atual = 45230.75
timestamp = "2025-12-16T19:30:00Z"
volume_24h = 1250000000.50

print("=== Dados da API Coinbase ===")
print(f"Par de moedas: {moeda}")
print(f"Preço atual: ${preco_atual:,.2f}")
print(f"Timestamp: {timestamp}")
print(f"Volume 24h: ${volume_24h:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🖨️ 2. Print
# MAGIC 
# MAGIC A função `print()` é uma das mais importantes em Python. Ela exibe informações na tela (ou no output do notebook).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print Básico

# COMMAND ----------

# Print simples
print("Olá, mundo!")
print("Bem-vindo ao pipeline de dados Bitcoin!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print com Variáveis

# COMMAND ----------

# Print com variáveis
nome_moeda = "Bitcoin"
preco = 45000

print("Moeda:", nome_moeda)
print("Preço:", preco)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print com Formatação (f-strings)
# MAGIC 
# MAGIC **f-strings** são a forma moderna e recomendada de formatar strings em Python!

# COMMAND ----------

# Usando f-strings (recomendado!)
nome = "Bitcoin"
preco = 45230.75
variacao = 2.5

print(f"Moeda: {nome}")
print(f"Preço: ${preco:,.2f}")
print(f"Variação: {variacao}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print com Múltiplas Linhas

# COMMAND ----------

# Print com múltiplas linhas
print("=== Relatório de Preços ===")
print(f"Moeda: Bitcoin")
print(f"Preço: $45,230.75")
print(f"Variação: +2.5%")
print("=" * 30)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Print de Estruturas de Dados

# COMMAND ----------

# Print de lista
precos = [45000, 45100, 45230, 45300]
print("Histórico de preços:", precos)

# Print de dicionário
dados_bitcoin = {
    "moeda": "BTC-USD",
    "preco": 45230.75,
    "volume": 1250000000
}
print("\nDados completos:")
print(dados_bitcoin)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## 🔧 3. Métodos
# MAGIC 
# MAGIC Métodos são funções que pertencem a objetos. Eles nos permitem realizar ações com os dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Métodos de String

# COMMAND ----------

# Exemplos de métodos de string
moeda = "bitcoin"

print(f"Original: {moeda}")
print(f"Upper (maiúsculas): {moeda.upper()}")
print(f"Capitalize (primeira maiúscula): {moeda.capitalize()}")
print(f"Replace: {moeda.replace('bitcoin', 'BTC')}")

# COMMAND ----------

# Métodos úteis para dados de API
par_moeda = "BTC-USD"

print(f"Par original: {par_moeda}")
print(f"Split por '-': {par_moeda.split('-')}")
print(f"Starts with 'BTC': {par_moeda.startswith('BTC')}")
print(f"Contains 'USD': {'USD' in par_moeda}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Métodos de Lista

# COMMAND ----------

# Métodos de lista
precos = [45000, 45100, 45230]

print(f"Lista original: {precos}")
print(f"Tamanho: {len(precos)}")
print(f"Último preço: {precos[-1]}")

# Adicionar elemento
precos.append(45300)
print(f"Após append: {precos}")

# Remover elemento
precos.remove(45100)
print(f"Após remove: {precos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Métodos de Dicionário

# COMMAND ----------

# Métodos de dicionário
dados = {
    "moeda": "BTC-USD",
    "preco": 45230.75,
    "volume": 1250000000
}

print("=== Métodos de Dicionário ===")
print(f"Chaves: {dados.keys()}")
print(f"Valores: {dados.values()}")
print(f"Items: {dados.items()}")

# Acessar valores
print(f"\nPreço: {dados.get('preco')}")
print(f"Timestamp: {dados.get('timestamp', 'Não disponível')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Métodos Úteis para ETL

# COMMAND ----------

# Simulando dados brutos da API
dados_brutos = {
    "data": {
        "base": "BTC",
        "currency": "USD",
        "amount": "45230.75"
    }
}

# Extrair e transformar dados
moeda_base = dados_brutos["data"]["base"]
moeda_quote = dados_brutos["data"]["currency"]
preco_str = dados_brutos["data"]["amount"]

# Converter string para float
preco_float = float(preco_str)

# Criar novo dicionário formatado
dados_formatados = {
    "par": f"{moeda_base}-{moeda_quote}",
    "preco": preco_float,
    "timestamp": "2025-12-16T19:30:00Z"
}

print("=== Dados Transformados ===")
for chave, valor in dados_formatados.items():
    print(f"{chave}: {valor}")

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
preco = float(dados_api["data"]["amount"])

# TODO: Crie uma string formatada com o par de moedas
par_moeda = f"{dados_api['data']['base']}-{dados_api['data']['currency']}"

# TODO: Use print com f-string para exibir os dados
print(f"=== Dados Processados ===")
print(f"Par de moedas: {par_moeda}")
print(f"Preço: ${preco:,.2f}")
print(f"Timestamp: {dados_api['timestamp']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC 
# MAGIC ## ✅ Resumo
# MAGIC 
# MAGIC Neste aquecimento, você aprendeu:
# MAGIC 
# MAGIC 1. **Variáveis**: Como armazenar dados em Python
# MAGIC 2. **Print**: Como exibir informações na tela
# MAGIC 3. **Métodos**: Como usar funções que pertencem a objetos
# MAGIC 
# MAGIC Esses são os fundamentos que você vai usar durante todo o projeto de ETL!
# MAGIC 
# MAGIC 🚀 **Pronto para começar o pipeline? Vamos lá!**

# COMMAND ----------

