# src/processing/transformations.py
from pyspark.sql import functions as F
import logging

logger = logging.getLogger(__name__)


class Transformation:
    """
    Classe que contém as transformações e regras de negócio da aplicação.
    """

    def gerar_relatorio(self, pedidos_df, pagamentos_df):

        pagamentos_filtrados_df = pagamentos_df.filter(
            (~F.col("status")) & (~F.col("avaliacao_fraude.fraude"))
        )

        relatorio_df = (
            pedidos_df.join(pagamentos_filtrados_df, "id_pedido", how="inner")
            .filter(F.year("data_criacao") == 2025)
            .withColumn("valor_total", F.col("valor_unitario") * F.col("quantidade"))
            .select("id_pedido", "uf", "forma_pagamento", "valor_total", "data_criacao")
            .orderBy("uf", "forma_pagamento", "data_criacao")
        )

        return relatorio_df
