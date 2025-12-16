<p align="center">
  <a href="https://suajornadadedados.com.br/"><img src="https://github.com/lvgalvao/data-engineering-roadmap/raw/main/pics/logo.png" alt="Jornada de Dados"></a>
</p>
<p align="center">
  <em>Nossa missão é fornecer o melhor ensino em engenharia de dados</em>
</p>

<p align="center">
  <img src="img/SeUZPWKQ.png" alt="Data Pipeline Bitcoin - ETL com Python e Databricks" width="800">
</p>

---

# 💰 **Data Pipeline: Extração de Dados Bitcoin com ETL em Python**

## 📋 **Sobre o Projeto**

Este projeto faz parte de um **workshop gratuito de Data Engineering para Iniciantes**, realizado no dia **16/12/2025 às 19h30**, que pode ser assistido aqui: [YouTube Live](https://www.youtube.com/live/pFJCL1S3Zj8)

O projeto é focado na criação de pipelines de dados **ETL (Extract, Transform, Load)** do zero. O objetivo é construir um programa completo que consome dados de uma **API** (Coinbase), organiza esses dados e armazena em diferentes formatos (JSON, Parquet, Delta Lake), além de criar visualizações e agentes de IA no Databricks.

### 🎯 **O que você vai aprender:**

- ✅ O que é uma API e como consumi-la usando Python
- ✅ O processo completo de ETL (extração, transformação e carga)
- ✅ Diferenças entre formatos de armazenamento (Row-based vs Columnar)
- ✅ Como automatizar a execução do pipeline para coleta contínua
- ✅ Criação de dashboards no Databricks
- ✅ Implementação de agentes de IA para análise de dados

### 🚀 **Resultado Final**

Ao final do projeto, você terá:
- ✅ Um programa funcional de pipeline ETL
- ✅ Dashboard interativo no Databricks
- ✅ Agente de IA para monitoramento e análise de preços

---

## 📊 **Esquema do Projeto**

Visualize a arquitetura completa do projeto: [app.excalidraw.com](https://app.excalidraw.com/s/8pvW6zbNUnD/9zZctm3OR9f)

---

## 🎯 **Overview do Projeto**

### **Objetivo Principal**

Desenvolver um pipeline ETL automatizado para consumir dados da **API da Coinbase** e armazenar informações sobre o preço da Bitcoin de forma estruturada e escalável.

### **Etapas do Projeto**

#### 1. **Extração (E)**

- Utilizar a **API da Coinbase** para obter o preço atual da Bitcoin
- Implementar tratamento de erros e retry logic
- Coleta de dados em tempo real

#### 2. **Carga (L)**

- Armazenar dados brutos em arquivo **JSON** (formato legível)
- Persistência inicial dos dados extraídos

#### 3. **Transformação (T)**

- Selecionar apenas as informações relevantes: preço da Bitcoin, horário da consulta e moeda de referência (USD)
- Organizar os dados utilizando **PySpark**
- Aplicar transformações e validações de dados
- Converter para formatos otimizados (Parquet, Delta Lake)

#### 4. **Visualização**

- Criar um dashboard usando o **Databricks** para monitorar os preços em tempo real
- Gráficos interativos e análises visuais

#### 5. **Agente de IA**

- Criar um agente de IA usando o **Databricks** para monitorar os preços em tempo real
- Análises automatizadas e insights inteligentes

---

## 🔧 **Stack Tecnológico**

Neste projeto, utilizamos bibliotecas essenciais para qualquer Engenheiro de Dados. Entenda o porquê de cada uma:

### **1. Requests** (`requests`)

- **O que faz**: É a biblioteca mais popular do Python para fazer requisições HTTP
- **Por que usamos**: Para "conversar" com a API da Coinbase. Ela envia o pedido (GET) e recebe a resposta com os dados do Bitcoin. É a porta de entrada dos dados no nosso pipeline

### **2. Pandas** (`pandas`)

- **O que faz**: Biblioteca fundamental para manipulação e análise de dados em Python
- **Por que usamos**: Para estruturar e transformar os dados extraídos da API antes de salvá-los em diferentes formatos

### **3. PySpark** (`pyspark`)

- **O que faz**: API Python para Apache Spark, framework de processamento distribuído
- **Por que usamos**: Para processar grandes volumes de dados de forma distribuída e preparar os dados para o Databricks

### **4. PyArrow** (`pyarrow`)

- **O que faz**: Biblioteca para processamento de dados colunares e intercâmbio entre sistemas
- **Por que usamos**: O Pandas precisa dele para salvar arquivos no formato **Parquet**. O Parquet é crucial em Big Data porque comprime os dados e permite leitura rápida, sendo o formato nativo de Data Lakes

### **5. Databricks**

- **O que faz**: Plataforma unificada de análise de dados baseada em Spark
- **Por que usamos**: Para criar dashboards, agentes de IA e processar dados em escala empresarial

---

## 📚 **Por Que Usar Databricks e PySpark?**

Embora este projeto rode localmente, ele prepara você para ambientes de Big Data como o **Databricks**. No mundo corporativo, lidamos com terabytes de dados.

- **PySpark**: É a API Python para o Apache Spark. Ao contrário do Pandas, que roda em uma única máquina, o Spark processa dados de forma distribuída em um cluster de computadores
- **Databricks**: É uma plataforma unificada de análise de dados baseada em Spark. Ela facilita a criação de Data Lakes e Data Warehouses modernos (Lakehouse)

### **Comparação de Formatos de Armazenamento**

#### 📄 **Arquivos de Texto (Text Files)**

São arquivos legíveis por humanos, cujo conteúdo é texto puro (ASCII / UTF-8).

**Exemplos clássicos:**

- CSV
- JSON
- TXT
- XML
- YAML

**Características:**

- ✅ Dá para abrir no bloco de notas
- ✅ Fácil de debugar
- ❌ Maior tamanho
- ❌ Leitura e escrita mais lentas em grandes volumes

#### 🧱 **Arquivos Binários (Binary Files)**

São arquivos não legíveis diretamente por humanos, otimizados para máquinas.

**Exemplos:**

- JPEG / PNG / MP3 / MP4
- **Parquet**
- ORC
- Avro
- PDF (em geral)
- Executáveis (.exe)

**Características:**

- ✅ Compactados
- ✅ Estruturados internamente
- ✅ Muito mais eficientes para processamento
- ⚠️ Exigem um software/biblioteca para leitura

#### 🎯 **E o Parquet?**

👉 **Sim, Parquet é um arquivo binário.** ✔️

Mais do que isso:

- É um arquivo binário **columnar**
- Otimizado para:
  - Leitura por coluna
  - Compressão
  - Analytics (Big Data, Data Warehouses, Lakehouse)

**Frase profissional:**
> "O Parquet é um formato binário e columnar, otimizado para processamento analítico e grandes volumes de dados."

**Explicação didática:**
> "De forma geral, podemos dividir os arquivos em dois grandes grupos: arquivos de texto, como CSV e JSON, que são legíveis por humanos; e arquivos binários, como imagens, vídeos e formatos analíticos como o Parquet, que são otimizados para processamento por máquinas."

### **Comparação Rápida**

| Formato | Tipo | Legível por humano? |
|---------|------|---------------------|
| CSV | Texto | ✅ |
| JSON | Texto | ✅ |
| TXT | Texto | ✅ |
| Excel (.xls/.xlsx) | Binário | ❌ |
| Parquet | Binário | ❌ |
| JPEG | Binário | ❌ |

---

## 🌐 **O Mundo Real: Databricks**

Embora este projeto rode no seu computador, ele foi desenhado para simular o que acontece em grandes empresas que usam **Databricks**.

### **O que é o Databricks?**

O Databricks é uma plataforma de análise de dados baseada em nuvem, criada pelos fundadores do Apache Spark. Ela unifica **Engenharia de Dados**, **Ciência de Dados** e **Machine Learning** em um único lugar (Lakehouse).

### **Por que ele é importante?**

1. **Processamento em Escala**: Enquanto o Pandas processa megabytes na sua RAM, o Databricks (via Spark) processa petabytes distribuídos em centenas de computadores

2. **Colaboração**: Notebooks compartilhados (como o Jupyter) permitem que times trabalhem juntos

3. **Modern Data Stack**: Ele incentiva o uso de **Parquet/Delta Lake** (que estamos simulando aqui) como padrão de armazenamento, garantindo performance e confiabilidade

Neste workshop, você está aprendendo os **fundamentos** (ETL, formatos de arquivo, APIs) que são exatamente os mesmos usados dentro do Databricks, apenas em menor escala.

---

## 🚀 **Como Usar**

### **Pré-requisitos**

- Python 3.8 ou superior
- Conta no Databricks (gratuita para testes)
- Conhecimento básico de Python

### **Instalação**

```bash
# Clone o repositório
git clone https://github.com/seu-usuario/pipeline-api-bitcoin-com-databricks.git
cd pipeline-api-bitcoin-com-databricks

# Instale as dependências
pip install -r requirements.txt
```

### **Execução**

```bash
# Execute o pipeline
python main.py
```

---

## 📖 **Estrutura do Projeto**

```text
pipeline-api-bitcoin-com-databricks/
├── img/
│   └── SeUZPWKQ.png          # Imagem de capa do projeto
├── src/
│   ├── extract.py            # Módulo de extração da API
│   ├── transform.py          # Módulo de transformação
│   └── load.py               # Módulo de carga
├── notebooks/                # Notebooks do Databricks
├── data/                     # Dados gerados
│   ├── raw/                  # Dados brutos (JSON)
│   └── processed/            # Dados processados (Parquet)
├── requirements.txt          # Dependências do projeto
└── README.md                 # Este arquivo
```

---

## 🎓 **Workshop**

Este projeto foi desenvolvido durante um workshop ao vivo. Assista a gravação completa:

🔗 [YouTube Live - Workshop Data Engineering](https://www.youtube.com/live/pFJCL1S3Zj8)

**Data:** 16/12/2025 às 19h30

---

## 📝 **Licença**

Este projeto é parte do conteúdo educacional da **Jornada de Dados**.

---

## 👥 **Contato**

- **Website**: [suajornadadedados.com.br](https://suajornadadedados.com.br/)
- **YouTube**: [Canal Jornada de Dados](https://www.youtube.com/@JornadadeDados)

---

<p align="center">
  <em>Desenvolvido com ❤️ pela equipe Jornada de Dados</em>
</p>
