from pyspark.sql import SparkSession
from pyspark.slq .functions import
from_json,col
from pyspark.slq.types import
StructType,StructField,StringType,DoubleType

#Iniciar Spark

spark=SparkSession.builder \
    .appName("TransaccionesBancolombia")
\
    .getOrCreate()

#definir el esquema de los datos

esquema=StructType([
    StructField("id_transaccion",StringType(), True),
    StructField("fecha",StringType(), True),
    StructField("hora",StringType(), True),
    StructField("valor",DoubleType(), True),
    StructField("tipo",StringType(), True),
    StructField("sucursal",StringType(), True),
    StructField("estado",StringType(), True),
])
print("conectando a kafka")

#leer el flujo de datos desde Kafka

df=spark.readStream \
   .format("kafka") \
   .option("kafka.bootstrap.servers","localhost:9092") \
   .option("subscribe","transacciones_bancolombia") \
   .load()

#Convertir los datos binarios a texto y luego a columnas

datos=df.selectExpr("CAST(value AS ATRING)") \
    .select(from_json(col("value"),esquema).alias("datos"))
    .select("datos.*)

#Mostrar los resultados en consola

consulta=datos.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()
print("Esperando transacciones")
print("="*70)
