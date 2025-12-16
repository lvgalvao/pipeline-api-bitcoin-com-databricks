# Databricks notebook source
# MAGIC %md
# MAGIC # Aquecimento: Fundamentos de Python
# MAGIC
# MAGIC Bem-vindo ao aquecimento! Este notebook vai revisar os conceitos fundamentais de Python que você precisa conhecer antes de começar o projeto de ETL.
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Variáveis
# MAGIC
# MAGIC Variáveis são como "caixas" onde guardamos informações. Em Python, você não precisa declarar o tipo da variável - o Python descobre automaticamente!

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Tipos de Variáveis Simples
# MAGIC
# MAGIC Python tem quatro tipos de dados básicos e simples que você vai usar constantemente:

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
# MAGIC #### Bool (bool) - Valores Booleanos
# MAGIC
# MAGIC Booleanos representam valores de verdade: `True` ou `False`. Usados em condições e validações.

# COMMAND ----------

# Exemplos de booleanos
ativo = True
disponivel = False
preco_maior_que_45000 = True

print(f"Ativo: {ativo}")
print(f"Disponível: {disponivel}")
print(f"Preço > 45000: {preco_maior_que_45000}")

# Verificando o tipo
print(f"\nTipo de 'ativo': {type(ativo)}")

# Operações com booleanos
print(f"Negado: {not ativo}")
print(f"E (AND): {ativo and disponivel}")
print(f"Ou (OR): {ativo or disponivel}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Resumo dos Tipos Simples

# COMMAND ----------

# Criando variáveis de cada tipo
nome = "Bitcoin"           # str
preco = 45000.50           # float
quantidade = 10            # int
ativo = True               # bool

# Verificando todos os tipos
print("=== Tipos de Variáveis ===")
print(f"nome = '{nome}' → Tipo: {type(nome).__name__}")
print(f"preco = {preco} → Tipo: {type(preco).__name__}")
print(f"quantidade = {quantidade} → Tipo: {type(quantidade).__name__}")
print(f"ativo = {ativo} → Tipo: {type(ativo).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Variáveis com Dados da API
# MAGIC
# MAGIC Vamos simular dados que viriam da API da Coinbase usando os tipos simples:

# COMMAND ----------

# Simulando dados da API usando tipos simples
moeda = "BTC-USD"                    # str
preco_atual = 45230.75               # float
timestamp = "2025-12-16T19:30:00Z"  # str
volume_24h = 1250000000.50           # float
transacoes = 15000                    # int
ativo = True                          # bool

print("=== Dados da API Coinbase ===")
print(f"Par de moedas: {moeda}")
print(f"Preço atual: ${preco_atual:,.2f}")
print(f"Timestamp: {timestamp}")
print(f"Volume 24h: ${volume_24h:,.2f}")
print(f"Transações: {transacoes:,}")
print(f"Ativo: {ativo}")

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
# MAGIC ---
# MAGIC
# MAGIC ## 1.4 Variáveis Compostas: Listas e Dicionários
# MAGIC
# MAGIC Além dos tipos simples, Python tem estruturas de dados mais complexas que permitem armazenar múltiplos valores.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lista (list) - Coleção Ordenada
# MAGIC
# MAGIC Listas são coleções ordenadas de itens. Podem conter qualquer tipo de dado, inclusive outras listas.

# COMMAND ----------

# Criando listas
precos = [45000, 45100, 45230, 45300]
moedas = ["Bitcoin", "Ethereum", "Litecoin"]
dados_mistos = ["BTC-USD", 45230.75, True]

print("=== Exemplos de Listas ===")
print(f"Preços: {precos}")
print(f"Moedas: {moedas}")
print(f"Dados mistos: {dados_mistos}")

# Verificando o tipo
print(f"\nTipo de 'precos': {type(precos)}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Acessando Elementos de uma Lista
# MAGIC
# MAGIC Listas são indexadas começando do zero. Você pode acessar elementos por posição.

# COMMAND ----------

precos = [45000, 45100, 45230, 45300]

print(f"Lista completa: {precos}")
print(f"Primeiro preço (índice 0): {precos[0]}")
print(f"Segundo preço (índice 1): {precos[1]}")
print(f"Último preço (índice -1): {precos[-1]}")
print(f"Penúltimo preço (índice -2): {precos[-2]}")

# Tamanho da lista
print(f"\nTamanho da lista: {len(precos)}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Operações com Listas

# COMMAND ----------

precos = [45000, 45100, 45230]

print(f"Lista original: {precos}")

# Adicionar elemento no final
precos.append(45300)
print(f"Após append(45300): {precos}")

# Adicionar elemento em posição específica
precos.insert(1, 45050)
print(f"Após insert(1, 45050): {precos}")

# Remover elemento
precos.remove(45100)
print(f"Após remove(45100): {precos}")

# Último elemento (remove e retorna)
ultimo = precos.pop()
print(f"Último elemento removido: {ultimo}")
print(f"Lista após pop(): {precos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dicionário (dict) - Pares Chave-Valor
# MAGIC
# MAGIC Dicionários armazenam dados em pares chave-valor. São muito úteis para representar dados estruturados, como respostas de APIs.

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
# MAGIC #### Acessando Valores de um Dicionário
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
# MAGIC #### Operações com Dicionários

# COMMAND ----------

dados = {
    "moeda": "BTC-USD",
    "preco": 45230.75
}

print("=== Operações com Dicionário ===")
print(f"Dicionário original: {dados}")

# Obter todas as chaves
print(f"\nChaves: {list(dados.keys())}")

# Obter todos os valores
print(f"Valores: {list(dados.values())}")

# Obter pares chave-valor
print(f"Items: {list(dados.items())}")

# Adicionar novo par chave-valor
dados["volume"] = 1250000000
print(f"\nApós adicionar 'volume': {dados}")

# Atualizar valor existente
dados["preco"] = 45300.00
print(f"Após atualizar 'preco': {dados}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dicionários Aninhados
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

# Print formatado de dicionário
print("\n=== Dados Formatados ===")
for chave, valor in dados_bitcoin.items():
    print(f"{chave}: {valor}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 3. Métodos
# MAGIC
# MAGIC Métodos são funções que pertencem a objetos. Eles nos permitem realizar ações com os dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## 🔧 3. Métodos
# MAGIC
# MAGIC Métodos são funções que pertencem a objetos. Eles nos permitem realizar ações com os dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Métodos de String

# COMMAND ----------

# Exemplos de métodos de string
moeda = "bitcoin"

print(f"Original: {moeda}")
print(f"Upper (maiúsculas): {moeda.upper()}")
print(f"Capitalize (primeira maiúscula): {moeda.capitalize()}")
print(f"Replace: {moeda.replace('bitcoin', 'BTC')}")

# Métodos úteis para dados de API
par_moeda = "BTC-USD"

print(f"\nPar original: {par_moeda}")
print(f"Split por '-': {par_moeda.split('-')}")
print(f"Starts with 'BTC': {par_moeda.startswith('BTC')}")
print(f"Contains 'USD': {'USD' in par_moeda}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Métodos de Lista

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
# MAGIC ### 3.3 Métodos de Dicionário

# COMMAND ----------

# Métodos de dicionário
dados = {
    "moeda": "BTC-USD",
    "preco": 45230.75,
    "volume": 1250000000
}

print("=== Métodos de Dicionário ===")
print(f"Chaves: {list(dados.keys())}")
print(f"Valores: {list(dados.values())}")
print(f"Items: {list(dados.items())}")

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
print("=== Dados Processados ===")
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
