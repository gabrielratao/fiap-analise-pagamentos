# src/processing/transformations.py

import logging

from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class Transformation:
    """
    Classe que contém as transformações e regras de negócio da aplicação.
    """

    def gerar_relatorio(self, pedidos_df, pagamentos_df):
        try:
            logger.info(
                "Iniciando geração do relatório de pedidos recusados legítimos."
            )

            logger.info("Filtrando pagamentos recusados e sem fraude.")
            pagamentos_filtrados_df = pagamentos_df.filter(
                (~F.col("status")) & (~F.col("avaliacao_fraude.fraude"))
            )

            logger.info(
                "Realizando join, filtro de ano, cálculo do valor total e ordenação."
            )
            relatorio_df = (
                pedidos_df.join(pagamentos_filtrados_df, "id_pedido", how="inner")
                .filter(F.year("data_criacao") == 2025)
                .withColumn(
                    "valor_total", F.col("valor_unitario") * F.col("quantidade")
                )
                .select(
                    "id_pedido",
                    "uf",
                    "forma_pagamento",
                    "valor_total",
                    "data_criacao",
                )
                .orderBy("uf", "forma_pagamento", "data_criacao")
            )

            logger.info("Relatório gerado com sucesso.")
            return relatorio_df

        except Exception:
            logger.exception("Erro ao gerar relatório de pedidos recusados legítimos.")
            raise
