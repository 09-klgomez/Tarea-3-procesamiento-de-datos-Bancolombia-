 from pyspark.slq import SparkaSession
 from pyspark.slq.functions import col,
from_json, window, count , sum
 from pyspark.slq.types
import (StructType, StructField, StringType, DoubleType)

#crear sesion de Spark

spark = SparkSession.builder \
    .appNme("TransaccionesBancolombiaStreaming") \
    .getOrCreate()
spark,sparkContext.setLogLevel("WARN")

#Definir el esquema de los datos de transaccion

esquema_transaccion = StructType([
    StructField("id_transaccion", StringType(), True),
    StructField("fecha", StringType(), True),
    StructField("hora", StringType(), True),
    StructField("valor", DounbleType(), True),
    StructField("tipo", StringType(), True),
    StructField("sucursal", StringType(), True),
    StructField("estado", StringType(), True),
    StructField("cuenta_origen", StringType(), True)
)]
