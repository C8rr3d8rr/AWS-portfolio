import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, dayofmonth
from awsglue.dynamicframe import DynamicFrame

# ===============================
# ARGUMENTOS DEL JOB
# ===============================
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ===============================
# LECTURA DESDE EL CATALOGO GLUE
# ===============================
dyf_input = glueContext.create_dynamic_frame.from_catalog(
    database="db-prueba-1",
    table_name="myprimerintento_origen",
    transformation_ctx="dyf_input"
)

# ===============================
# TRANSFORMACIONES
# ===============================
# Convertir a DataFrame de Spark
df_input = dyf_input.toDF()

# Eliminar las columnas texto_1 ... texto_6
cols_to_drop = ["texto_1", "texto_2", "texto_3", "texto_4", "texto_5", "texto_6"]
df_clean = df_input.drop(*cols_to_drop)

# Agregar columnas de partición basadas en la columna FECHA
df_partitioned = df_clean.withColumn("year", year(col("FECHA"))) \
                         .withColumn("month", month(col("FECHA"))) \
                         .withColumn("day", dayofmonth(col("FECHA")))

# Convertir de nuevo a DynamicFrame
dyf_partitioned = DynamicFrame.fromDF(df_partitioned, glueContext, "dyf_partitioned")

# ===============================
# ESCRITURA EN S3
# ===============================
glueContext.write_dynamic_frame.from_options(
    frame=dyf_partitioned,
    connection_type="s3",
    connection_options={
        "path": "s3://myprimerintento-destino/clean-data/",
        "partitionKeys": ["year", "month", "day"]   # Particionado dinámico
    },
    format="parquet",   # o "csv"
    transformation_ctx="write_output"
)

job.commit()
