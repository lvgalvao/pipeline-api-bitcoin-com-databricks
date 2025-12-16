# Databricks notebook source
# MAGIC %md
# MAGIC # Extração de Dados Bitcoin - API Coinbase
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
# import json
# from datetime import datetime

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

resposta = requests.get(url)

# COMMAND ----------

print(resposta.json())

# COMMAND ----------

def extrair_dados_bitcoin():
    """Extrai o JSON completo da API da Coinbase."""
    url = 'https://api.coinbase.com/v2/prices/spot'
    resposta = requests.get(url)
    return resposta.json()

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
    timestamp_readable = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    dados_tratados = [{
        "valor": valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda,
        "timestamp": timestamp,
        "timestamp_readable": timestamp_readable
    }]
    
    return dados_tratados

# Tratando os dados
dados_bitcoin = tratar_dados_bitcoin(dados_json)
print("✅ Dados tratados com sucesso!")
print(f"\nDados processados:")
print(json.dumps(dados_bitcoin, indent=2, ensure_ascii=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Configurando noVolume no Databricks
# MAGIC
# MAGIC O que são Volumes no Databricks?
# MAGIC
# MAGIC **Volumes** são locais de armazenamento gerenciados pelo Databricks que permitem:
# MAGIC - ✅ **Organização**: Estruturar dados de forma hierárquica
# MAGIC - ✅ **Acesso Fácil**: Montados diretamente no sistema de arquivos
# MAGIC - ✅ **Colaboração**: Compartilhados entre usuários e notebooks
# MAGIC - ✅ **Performance**: Otimizados para leitura/escrita
# MAGIC
# MAGIC **Formato do caminho**: `/Volumes/<catalog>/<schema>/<volume>/<path>`
# MAGIC
# MAGIC No nosso caso, vamos usar: `/Volumes/main/default/bitcoin_data`

# COMMAND ----------

# Definindo caminho do volume no Databricks
# Formato: /Volumes/<catalog>/<schema>/<volume>/<path>
volume_path = "/Volumes/main/default/bitcoin_data"

# Criando estrutura de diretórios
directories = {
    "json": f"{volume_path}/json",
    "parquet": f"{volume_path}/parquet",
    "csv": f"{volume_path}/csv"
}

# Criando diretórios se não existirem
for dir_type, dir_path in directories.items():
    os.makedirs(dir_path, exist_ok=True)
    print(f"✅ Diretório {dir_type}: {dir_path}")

print(f"\n📁 Volume configurado: {volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Salvando e Atualizando JSON
# MAGIC
# MAGIC JSON é um formato de texto legível por humanos, ideal para armazenar dados estruturados de forma simples.

# COMMAND ----------

# Caminho do arquivo JSON no volume do Databricks
json_file = f"{directories['json']}/bitcoin_data.json"

# Carregar dados existentes (se houver) - isso permite atualização incremental
dados_existentes = []
if os.path.exists(json_file):
    with open(json_file, 'r', encoding='utf-8') as f:
        dados_existentes = json.load(f)
    print(f"📂 Arquivo JSON existente encontrado com {len(dados_existentes)} registros")

# Adicionar novo registro (dados_bitcoin é uma lista)
dados_existentes.extend(dados_bitcoin)

# Salvar JSON atualizado no volume
with open(json_file, 'w', encoding='utf-8') as f:
    json.dump(dados_existentes, f, indent=2, ensure_ascii=False)

print(f"✅ JSON atualizado e salvo no volume: {json_file}")
print(f"📊 Total de registros: {len(dados_existentes)}")

# Mostrar últimos 3 registros
print(f"\nÚltimos 3 registros:")
for registro in dados_existentes[-3:]:
    valor_formatado = float(registro['valor'])
    print(f"  - {registro['timestamp_readable']}: ${valor_formatado:,.2f} {registro['moeda']}")

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

# Criar DataFrame do Pandas a partir dos dados tratados
df = pd.DataFrame(dados_bitcoin)

# Mostrar DataFrame
print("📊 DataFrame criado:")
print(df)
print(f"\nTipos de dados:")
print(df.dtypes)

# Caminho do arquivo Parquet no volume do Databricks
parquet_file = f"{directories['parquet']}/bitcoin_data.parquet"

# Verificar se arquivo Parquet já existe para fazer append (atualização incremental)
if os.path.exists(parquet_file):
    # Ler Parquet existente
    df_existente = pd.read_parquet(parquet_file)
    # Concatenar com novos dados
    df_parquet_final = pd.concat([df_existente, df], ignore_index=True)
    print(f"📂 Parquet existente encontrado com {len(df_existente)} registros")
else:
    df_parquet_final = df
    print("📝 Criando novo arquivo Parquet")

# Salvar como Parquet no volume
df_parquet_final.to_parquet(parquet_file, index=False, engine='pyarrow')

# Verificar tamanho do arquivo
tamanho_parquet = os.path.getsize(parquet_file)
print(f"\n✅ Parquet salvo no volume: {parquet_file}")
print(f"📦 Tamanho do arquivo: {tamanho_parquet:,} bytes ({tamanho_parquet/1024:.2f} KB)")
print(f"📊 Total de registros no Parquet: {len(df_parquet_final)}")

# Ler e mostrar dados do Parquet para confirmar
df_parquet_leitura = pd.read_parquet(parquet_file)
print(f"\n✅ Últimas 3 linhas lidas do Parquet:")
print(df_parquet_leitura.tail(3))

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

# Caminho do arquivo CSV no volume do Databricks
csv_file = f"{directories['csv']}/bitcoin_data.csv"

# Criar DataFrame a partir dos dados tratados
df_csv_novo = pd.DataFrame(dados_bitcoin)

# Verificar se arquivo CSV já existe (atualização incremental)
if os.path.exists(csv_file):
    # Ler CSV existente e adicionar nova linha
    df_csv_existente = pd.read_csv(csv_file)
    df_csv_atualizado = pd.concat([df_csv_existente, df_csv_novo], ignore_index=True)
    print(f"📂 CSV existente encontrado com {len(df_csv_existente)} registros")
else:
    # Criar novo CSV
    df_csv_atualizado = df_csv_novo
    print("📝 Criando novo arquivo CSV")

# Salvar CSV no volume (append mode ou criar novo)
df_csv_atualizado.to_csv(csv_file, index=False, encoding='utf-8')

# Verificar tamanho do arquivo
tamanho_csv = os.path.getsize(csv_file)

print(f"\n✅ CSV salvo no volume: {csv_file}")
print(f"📦 Tamanho do arquivo: {tamanho_csv:,} bytes ({tamanho_csv/1024:.2f} KB)")
print(f"📊 Total de registros no CSV: {len(df_csv_atualizado)}")

# Mostrar últimas linhas
print(f"\n✅ Últimas 3 linhas do CSV:")
print(df_csv_atualizado.tail(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Comparação Detalhada: CSV vs Parquet
# MAGIC
# MAGIC Vamos comparar os tamanhos dos arquivos e entender as diferenças práticas entre os dois formatos:

# COMMAND ----------

# Comparando tamanhos dos arquivos salvos no volume
parquet_file = f"{directories['parquet']}/bitcoin_data.parquet"
csv_file = f"{directories['csv']}/bitcoin_data.csv"

if os.path.exists(parquet_file) and os.path.exists(csv_file):
    tamanho_parquet = os.path.getsize(parquet_file)
    tamanho_csv = os.path.getsize(csv_file)
    
    print("=" * 60)
    print("COMPARAÇÃO: CSV vs PARQUET")
    print("=" * 60)
    
    print(f"\n📄 CSV:")
    print(f"   Tamanho: {tamanho_csv:,} bytes ({tamanho_csv/1024:.2f} KB)")
    
    print(f"\n📊 Parquet:")
    print(f"   Tamanho: {tamanho_parquet:,} bytes ({tamanho_parquet/1024:.2f} KB)")
    
    if tamanho_csv > 0:
        economia = ((tamanho_csv - tamanho_parquet) / tamanho_csv) * 100
        print(f"\n💾 Economia com Parquet: {economia:.1f}%")
        print(f"   (CSV é {tamanho_csv/tamanho_parquet:.1f}x maior que Parquet)")
    
    print("\n" + "=" * 60)
    print("DIFERENÇAS PRÁTICAS")
    print("=" * 60)
    
    print("\n📄 CSV (Comma-Separated Values):")
    print("  ✅ Legível por humanos (Excel, Bloco de Notas)")
    print("  ✅ Fácil de debugar e inspecionar")
    print("  ✅ Universal (suportado por todas as ferramentas)")
    print("  ✅ Row-based (otimizado para transações)")
    print("  ❌ Arquivo maior (sem compressão)")
    print("  ❌ Leitura mais lenta em grandes volumes")
    print("  ❌ Não preserva tipos de dados (tudo é texto)")
    
    print("\n📊 Parquet (Apache Parquet):")
    print("  ✅ Arquivo menor (compressão eficiente)")
    print("  ✅ Leitura muito mais rápida (columnar)")
    print("  ✅ Mantém schema (tipos de dados)")
    print("  ✅ Ideal para Big Data e Analytics")
    print("  ✅ Columnar (otimizado para consultas analíticas)")
    print("  ❌ Não é legível diretamente por humanos")
    print("  ❌ Requer ferramentas especiais (Pandas, Spark)")
    
    print("\n" + "=" * 60)
    print("QUANDO USAR CADA UM?")
    print("=" * 60)
    print("\n📄 Use CSV quando:")
    print("  - Dados pequenos ou médios (< 100MB)")
    print("  - Precisa ser legível por humanos")
    print("  - Integração com Excel/ferramentas de negócio")
    print("  - Debugging e inspeção manual")
    
    print("\n📊 Use Parquet quando:")
    print("  - Grandes volumes de dados (> 100MB)")
    print("  - Performance é crítica")
    print("  - Data Lakes e Data Warehouses")
    print("  - Análises analíticas (OLAP)")
    print("  - Processamento com Spark/PySpark")
else:
    print("⚠️ Arquivos não encontrados. Execute as células anteriores primeiro.")

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

# COMMAND ----------


