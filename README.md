# Data Engineering Programming – Trabalho Final

## Descrição

Projeto desenvolvido para a disciplina **Data Engineering Programming**, utilizando **PySpark** e conceitos de Programação Orientada a Objetos (POO).

O objetivo é gerar um relatório contendo os pedidos de venda cujo pagamento foi recusado (`status = false`) e classificados como legítimos na avaliação de fraude (`fraude = false`).

O relatório considera apenas pedidos do ano de **2025**, contendo as seguintes informações:

- Identificador do pedido (`id_pedido`)
- Estado (`uf`)
- Forma de pagamento
- Valor total do pedido
- Data de criação do pedido

O resultado é ordenado por **UF**, **forma de pagamento** e **data de criação**, sendo gravado em formato **Parquet**.

---

## Estrutura do Projeto

```
.
├── data
│   ├── input
│   └── output
├── src
│   ├── config
│   ├── io_utils
│   ├── pipeline
│   ├── processing
│   ├── session
│   └── main.py
├── tests
│   └── unit
├── README.md
├── pyproject.toml
├── requirements.txt
└── MANIFEST.in
```

---

## Arquitetura

O projeto foi desenvolvido utilizando Programação Orientada a Objetos e Injeção de Dependências.

| Classe | Responsabilidade |
|--------|-------------------|
| **Settings** | Carrega as configurações da aplicação |
| **SparkSessionManager** | Gerencia a criação da SparkSession |
| **DataHandler** | Realiza a leitura e gravação dos dados |
| **Transformation** | Contém a lógica de negócio do relatório |
| **Pipeline** | Orquestra a execução do processamento |
| **main.py** | Instancia e injeta todas as dependências da aplicação |

---

## Pré-requisitos

Antes de executar o projeto, certifique-se de possuir:

- Python 3.11
- Java 11 ou superior
- Apache Spark (PySpark)

---

## Instalação

Clone o repositório:

```bash
git clone https://github.com/gabrielratao/fiap-analise-pagamentos.git
```

Acesse a pasta do projeto:

```bash
cd fiap-analise-pagamentos
```

Crie um ambiente virtual:

```bash
python -m venv .venv
```

Ative o ambiente virtual:

**Linux/macOS**

```bash
source .venv/bin/activate
```

**Windows**

```bash
.venv\Scripts\activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

---

## Datasets

Os datasets utilizados neste projeto estão disponíveis nos repositórios:

- https://github.com/infobarbosa/dataset-json-pagamentos
- https://github.com/infobarbosa/datasets-csv-pedidos

Após realizar o download, mantenha os arquivos nos caminhos configurados no arquivo `settings.yaml`.

---

## Configuração

As configurações da aplicação estão centralizadas no arquivo:

```
src/config/settings.yaml
```

Neste arquivo são definidos:

- Caminhos dos datasets de entrada;
- Caminho do diretório de saída;
- Configurações da SparkSession.

---

## Execução

Execute o projeto utilizando:

```bash
python src/main.py
```

Ao final da execução será gerado um relatório em formato **Parquet** no diretório configurado no arquivo `settings.yaml`.

---

## Testes

Para executar os testes unitários:

```bash
pytest
```

---

## Qualidade do Código

Verificação estática:

```bash
ruff check .
```

Formatação do código:

```bash
black .
```

---

## Tecnologias Utilizadas

- Python
- PySpark
- Pytest
- Ruff
- Black

---

## Repositório

GitHub:

https://github.com/gabrielratao/fiap-analise-pagamentos