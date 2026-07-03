# src/io_utils/data_handler.py
import logging
from py4j.protocol import Py4JJavaError
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    BooleanType,
    FloatType,
    TimestampType,
)

logger = logging.getLogger(__name__)


class DataHandler:
    """
    Classe responsável pela leitura (input) e escrita (output) de dados.
    """

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def _get_schema_pagamentos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pagamentos."""
        return StructType(
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

    def _get_schema_pedidos(self) -> StructType:
        """Define e retorna o schema para o dataframe de pedidos."""
        return StructType(
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

    def load_pagamentos(self, path: str) -> DataFrame:
        """Carrega o dataframe de pagamentos a partir de um arquivo JSON."""
        schema = self._get_schema_pagamentos()
        return self.spark.read.option("compression", "gzip").json(path, schema=schema)

    def load_pedidos(
        self, path: str, compression: str, header: bool, sep: str
    ) -> DataFrame:
        try:
            schema = self._get_schema_pedidos()
            df = (
                self.spark.read.option("compression", compression)
                .option("mode", "FAILFAST")
                .csv(path, header=header, schema=schema, sep=sep)
            )

            # Verificação de Dataframe Vazio
            if df.isEmpty():
                logger.warning(
                    f"ATENÇÃO: O arquivo em '{path}' foi lido mas não contém registros."
                )

            return df

        except AnalysisException as e:
            logger.error(f"Erro de IO/Spark: {e}")
            raise e

        except Py4JJavaError as e:
            logger.critical(
                f"Erro Crítico na JVM (possível arquivo corrompido ou erro de memória): {e}"
            )
            raise e

    def write_parquet(self, df: DataFrame, path: str):
        """
        Salva o DataFrame em formato Parquet, sobrescrevendo se já existir.

        :param df: DataFrame a ser salvo.
        :param path: Caminho de destino.
        """
        df.write.mode("overwrite").parquet(path)
        logger.info(f"Dados salvos com sucesso em: {path}")
