[DATAENG] Trabalho - Desafio Relatório de Pagamentos

# Data Engineering Programming – Trabalho Final

## Descrição

Projeto desenvolvido para a disciplina **Data Engineering Programming** utilizando **PySpark** e conceitos de Programação Orientada a Objetos (POO).

O objetivo é gerar um relatório contendo os pedidos de venda cujo pagamento foi recusado (`status = false`) e classificados como legítimos na avaliação de fraude (`fraude = false`).

O relatório gerado possui as seguintes informações:

* Identificador do pedido
* Estado (UF)
* Forma de pagamento
* Valor total do pedido
* Data do pedido

São considerados apenas pedidos do ano de **2025**.

O resultado é gravado no formato **Parquet**.

---

## Estrutura do projeto

```text
src/
├── config/
├── io_utils/
├── pipeline/
├── processing/
├── session/
└── main.py

tests/
└── unit/

data/
├── input/
└── output/
```

---

## Pré-requisitos

* Python 3.11
* Apache Spark (PySpark)
* Ambiente Linux/WSL ou compatível

---

## Instalação

Clone o repositório:

```bash
git clone https://github.com/gabrielratao/fiap-analise-pagamentos.git
cd fiap-analise-pagamentos
```

Crie e ative o ambiente virtual:

```bash
python -m venv .venv
source .venv/bin/activate
```

Instale as dependências:

```bash
pip install -r requirements.txt
```

---

## Datasets

Os datasets utilizados estão disponíveis nos repositórios:

* https://github.com/infobarbosa/dataset-json-pagamentos
* https://github.com/infobarbosa/datasets-csv-pedidos

Após o download, mantenha a estrutura de diretórios conforme definida no arquivo `settings.yaml`.

---

## Execução

Execute o projeto com:

```bash
python src/main.py
```

O relatório será gravado no diretório configurado no arquivo `settings.yaml`.

---

## Testes

Para executar os testes unitários:

```bash
pytest
```

---

## Verificação do código

Executar análise estática:

```bash
ruff check .
```

Formatar o código:

```bash
black .
```
