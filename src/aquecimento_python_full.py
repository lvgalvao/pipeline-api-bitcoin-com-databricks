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
nome_moeda = "Bitcoin"
simbolo = 'BTC'
par_moeda = "BTC-USD"
timestamp = "2025-12-16T19:30:00Z"

print(f"Nome: {nome_moeda}")
print(f"Símbolo: {simbolo}")
print(f"Par: {par_moeda}")
print(f"Timestamp: {timestamp}")

# Verificando o tipo
print(f"\nTipo de 'nome_moeda': {type(nome_moeda)}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Int (int) - Números Inteiros
# MAGIC
# MAGIC Inteiros são números sem parte decimal. Usados para contagens, índices, quantidades.

# COMMAND ----------

# Exemplos de inteiros
quantidade = 10
volume_transacoes = 1250
ano = 2025

print(f"Quantidade: {quantidade}")
print(f"Volume: {volume_transacoes}")
print(f"Ano: {ano}")

# Verificando o tipo
print(f"\nTipo de 'quantidade': {type(quantidade)}")

# Operações com inteiros
soma = quantidade + volume_transacoes
print(f"Soma: {soma}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Float (float) - Números Decimais
# MAGIC
# MAGIC Floats são números com parte decimal. Essenciais para valores monetários, preços, percentuais.

# COMMAND ----------

# Exemplos de floats
preco = 45000.50
variacao_percentual = 2.5
volume_24h = 1250000000.75

print(f"Preço: ${preco:,.2f}")
print(f"Variação: {variacao_percentual}%")
print(f"Volume 24h: ${volume_24h:,.2f}")

# Verificando o tipo
print(f"\nTipo de 'preco': {type(preco)}")

# Operações com floats
preco_com_taxa = preco * 1.01
print(f"Preço com taxa: ${preco_com_taxa:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Resumo dos Tipos Simples

# COMMAND ----------

# Criando variáveis de cada tipo
nome = "Bitcoin"           # str
preco = 45000.50           # float
quantidade = 10            # int

# Verificando todos os tipos
print("=== Tipos de Variáveis ===")
print(f"nome = '{nome}' → Tipo: {type(nome).__name__}")
print(f"preco = {preco} → Tipo: {type(preco).__name__}")
print(f"quantidade = {quantidade} → Tipo: {type(quantidade).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Variáveis com Dados da API
# MAGIC
# MAGIC Vamos simular dados que viriam da API da Coinbase usando os tipos simples:

# COMMAND ----------

# Simulando dados da API usando tipos simples
moeda = "BTC-USD"                    # str
preco_atual = 45230.75               # float
timestamp = "2025-12-16T19:30:00Z"  # str
volume_24h = 1250000000.50           # float
transacoes = 15000                    # int

print("=== Dados da API Coinbase ===")
print(f"Par de moedas: {moeda}")
print(f"Preço atual: ${preco_atual:,.2f}")
print(f"Timestamp: {timestamp}")
print(f"Volume 24h: ${volume_24h:,.2f}")
print(f"Transações: {transacoes:,}")

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
dados_bitcoin = {
    "moeda": "BTC-USD",
    "preco": 45230.75,
    "volume": 1250000000
}

print("=== Exemplo de Dicionário ===")
print(dados_bitcoin)

# Verificando o tipo
print(f"\nTipo de 'dados_bitcoin': {type(dados_bitcoin)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Acessando Valores de um Dicionário
# MAGIC
# MAGIC Você pode acessar valores usando a chave entre colchetes ou o método `.get()`.

# COMMAND ----------

dados_bitcoin = {
    "moeda": "BTC-USD",
    "preco": 45230.75,
    "volume": 1250000000
}

# Acessando valores
print(f"Moeda: {dados_bitcoin['moeda']}")
print(f"Preço: ${dados_bitcoin['preco']:,.2f}")
print(f"Volume: ${dados_bitcoin['volume']:,.2f}")

# Usando get() (mais seguro - retorna None se a chave não existir)
print(f"\nPreço (com get): ${dados_bitcoin.get('preco'):,.2f}")
print(f"Timestamp (com get): {dados_bitcoin.get('timestamp', 'Não disponível')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Dicionários Aninhados
# MAGIC
# MAGIC Dicionários podem conter outros dicionários, o que é muito comum em respostas de APIs.

# COMMAND ----------

# Simulando resposta completa da API Coinbase
dados_api = {
    "data": {
        "base": "BTC",
        "currency": "USD",
        "amount": "45230.75"
    },
    "timestamp": "2025-12-16T19:30:00Z"
}

print("=== Dicionário Aninhado ===")
print(f"Estrutura completa:\n{dados_api}")

# Acessando valores aninhados
print(f"\nMoeda base: {dados_api['data']['base']}")
print(f"Moeda quote: {dados_api['data']['currency']}")
print(f"Preço: ${dados_api['data']['amount']}")
print(f"Timestamp: {dados_api['timestamp']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 4. Métodos Úteis para ETL
# MAGIC
# MAGIC Métodos são funções que pertencem a objetos. Vamos focar nos métodos mais úteis para trabalhar com dados de APIs.

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
print("=== Dados Processados ===")
print(f"Par de moedas: {par_moeda}")
print(f"Preço: ${preco:,.2f}")
print(f"Timestamp: {dados_api['timestamp']}")

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
def somar(a, b):
    """Função que soma dois números e retorna o resultado."""
    resultado = a + b
    return resultado

# Usando a função
numero1 = 10
numero2 = 20
soma = somar(numero1, numero2)

print(f"{numero1} + {numero2} = {soma}")

# Podemos usar diretamente também
print(f"5 + 3 = {somar(5, 3)}")

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
