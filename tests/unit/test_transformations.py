# tests/unit/test_transformations.py

from datetime import datetime

import pytest
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import (
    BooleanType,
    FloatType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from processing.transformations import Transformation


# --- Schemas reutilizáveis ---

SCHEMA_PEDIDOS = StructType(
    [
        StructField("id_pedido", StringType(), True),
        StructField("produto", StringType(), True),
        StructField("valor_unitario", FloatType(), True),
        StructField("quantidade", LongType(), True),
        StructField("data_criacao", TimestampType(), True),
        StructField("uf", StringType(), True),
        StructField("id_cliente", LongType(), True),
    ]
)

SCHEMA_PAGAMENTOS = StructType(
    [
        StructField("id_pedido", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("valor_pagamento", FloatType(), True),
        StructField("status", BooleanType(), True),
        StructField("data_processamento", TimestampType(), True),
        StructField(
            "avaliacao_fraude",
            StructType(
                [
                    StructField("fraude", BooleanType(), True),
                    StructField("score", FloatType(), True),
                ]
            ),
            True,
        ),
    ]
)

# Schema de pagamentos sem o campo "avaliacao_fraude", usado para simular
# uma fonte de dados inválida/incompleta no teste de tratamento de erro.
SCHEMA_PAGAMENTOS_SEM_AVALIACAO_FRAUDE = StructType(
    [
        StructField("id_pedido", StringType(), True),
        StructField("forma_pagamento", StringType(), True),
        StructField("valor_pagamento", FloatType(), True),
        StructField("status", BooleanType(), True),
        StructField("data_processamento", TimestampType(), True),
    ]
)


class TestGerarRelatorio:

    def test_resultado_contem_apenas_colunas_esperadas(self, spark):
        """O relatório final deve conter apenas as colunas necessárias."""
        pedidos = spark.createDataFrame(
            [
                ("p1", "TV", 100.0, 2, datetime(2025, 1, 10, 10, 0, 0), "SP", 1),
            ],
            SCHEMA_PEDIDOS,
        )

        pagamentos = spark.createDataFrame(
            [
                (
                    "p1",
                    "cartao",
                    200.0,
                    False,
                    datetime(2025, 1, 10, 11, 0, 0),
                    (False, 0.1),
                ),
            ],
            SCHEMA_PAGAMENTOS,
        )

        resultado = Transformation().gerar_relatorio(pedidos, pagamentos)

        assert resultado.columns == [
            "id_pedido",
            "uf",
            "forma_pagamento",
            "valor_total",
            "data_criacao",
        ]

    def test_filtra_apenas_pagamentos_recusados_e_sem_fraude(self, spark):
        """Só devem entrar pagamentos com status=False e fraude=False."""
        pedidos = spark.createDataFrame(
            [
                ("p1", "TV", 100.0, 2, datetime(2025, 1, 10, 10, 0, 0), "SP", 1),
                ("p2", "Mouse", 50.0, 1, datetime(2025, 1, 11, 10, 0, 0), "RJ", 2),
                ("p3", "Teclado", 80.0, 1, datetime(2025, 1, 12, 10, 0, 0), "MG", 3),
            ],
            SCHEMA_PEDIDOS,
        )

        pagamentos = spark.createDataFrame(
            [
                (
                    "p1",
                    "cartao",
                    200.0,
                    False,
                    datetime(2025, 1, 10, 11, 0, 0),
                    (False, 0.1),
                ),
                (
                    "p2",
                    "pix",
                    50.0,
                    True,
                    datetime(2025, 1, 11, 11, 0, 0),
                    (False, 0.2),
                ),
                (
                    "p3",
                    "boleto",
                    80.0,
                    False,
                    datetime(2025, 1, 12, 11, 0, 0),
                    (True, 0.9),
                ),
            ],
            SCHEMA_PAGAMENTOS,
        )

        resultado = Transformation().gerar_relatorio(pedidos, pagamentos)

        ids = [r.id_pedido for r in resultado.collect()]
        assert ids == ["p1"]

    def test_pagamento_com_status_nulo_e_excluido(self, spark):
        """
        status=null não deve ser tratado como recusado: ~F.col(null) resulta em
        null, e o filtro descarta a linha. Este teste documenta esse comportamento.
        """
        pedidos = spark.createDataFrame(
            [
                ("p1", "TV", 100.0, 2, datetime(2025, 1, 10, 10, 0, 0), "SP", 1),
            ],
            SCHEMA_PEDIDOS,
        )

        pagamentos = spark.createDataFrame(
            [
                (
                    "p1",
                    "cartao",
                    200.0,
                    None,
                    datetime(2025, 1, 10, 11, 0, 0),
                    (False, 0.1),
                ),
            ],
            SCHEMA_PAGAMENTOS,
        )

        resultado = Transformation().gerar_relatorio(pedidos, pagamentos)

        assert resultado.count() == 0

    def test_pagamento_com_fraude_nula_e_excluido(self, spark):
        """
        fraude=null também é descartado pelo mesmo motivo de status=null:
        ~F.col(null) não é considerado False pelo filtro do Spark.
        """
        pedidos = spark.createDataFrame(
            [
                ("p1", "TV", 100.0, 2, datetime(2025, 1, 10, 10, 0, 0), "SP", 1),
            ],
            SCHEMA_PEDIDOS,
        )

        pagamentos = spark.createDataFrame(
            [
                (
                    "p1",
                    "cartao",
                    200.0,
                    False,
                    datetime(2025, 1, 10, 11, 0, 0),
                    (None, 0.1),
                ),
            ],
            SCHEMA_PAGAMENTOS,
        )

        resultado = Transformation().gerar_relatorio(pedidos, pagamentos)

        assert resultado.count() == 0

    def test_pedido_com_multiplos_pagamentos_recusados_gera_multiplas_linhas(
        self, spark
    ):
        """
        Quando um pedido tem mais de uma tentativa de pagamento recusada e
        legítima, o join produz uma linha por tentativa (fan-out). Este teste
        trava esse comportamento para evitar regressões silenciosas.
        """
        pedidos = spark.createDataFrame(
            [
                ("p1", "TV", 50.0, 1, datetime(2025, 1, 11, 10, 0, 0), "RJ", 1),
            ],
            SCHEMA_PEDIDOS,
        )

        pagamentos = spark.createDataFrame(
            [
                (
                    "p1",
                    "pix",
                    50.0,
                    False,
                    datetime(2025, 1, 11, 11, 0, 0),
                    (False, 0.2),
                ),
                (
                    "p1",
                    "boleto",
                    50.0,
                    False,
                    datetime(2025, 1, 11, 12, 0, 0),
                    (False, 0.3),
                ),
            ],
            SCHEMA_PAGAMENTOS,
        )

        resultado = Transformation().gerar_relatorio(pedidos, pagamentos)

        formas_pagamento = sorted(r.forma_pagamento for r in resultado.collect())
        assert formas_pagamento == ["boleto", "pix"]

    def test_lanca_excecao_quando_pagamentos_sem_avaliacao_fraude(self, spark):
        """
        A classe deve propagar o erro (try/except + logging) quando o
        DataFrame de pagamentos não contém a coluna esperada avaliacao_fraude,
        em vez de falhar silenciosamente.
        """
        pedidos = spark.createDataFrame(
            [
                ("p1", "TV", 100.0, 2, datetime(2025, 1, 10, 10, 0, 0), "SP", 1),
            ],
            SCHEMA_PEDIDOS,
        )

        pagamentos_invalidos = spark.createDataFrame(
            [
                ("p1", "cartao", 200.0, False, datetime(2025, 1, 10, 11, 0, 0)),
            ],
            SCHEMA_PAGAMENTOS_SEM_AVALIACAO_FRAUDE,
        )

        with pytest.raises(AnalysisException):
            Transformation().gerar_relatorio(pedidos, pagamentos_invalidos)

    def test_filtra_apenas_pedidos_de_2025(self, spark):
        """Pedidos fora de 2025 não devem aparecer no relatório."""
        pedidos = spark.createDataFrame(
            [
                ("p1", "TV", 100.0, 2, datetime(2025, 1, 10, 10, 0, 0), "SP", 1),
                ("p2", "Mouse", 50.0, 1, datetime(2024, 1, 10, 10, 0, 0), "RJ", 2),
            ],
            SCHEMA_PEDIDOS,
        )

        pagamentos = spark.createDataFrame(
            [
                (
                    "p1",
                    "cartao",
                    200.0,
                    False,
                    datetime(2025, 1, 10, 11, 0, 0),
                    (False, 0.1),
                ),
                (
                    "p2",
                    "pix",
                    50.0,
                    False,
                    datetime(2024, 1, 10, 11, 0, 0),
                    (False, 0.2),
                ),
            ],
            SCHEMA_PAGAMENTOS,
        )

        resultado = Transformation().gerar_relatorio(pedidos, pagamentos)

        ids = [r.id_pedido for r in resultado.collect()]
        assert ids == ["p1"]
