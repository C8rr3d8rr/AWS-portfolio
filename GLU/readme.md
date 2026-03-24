# AWS
# Laboratorio AWS Glue
## Arquitectura
[Uploading Plantilla_Creación de Empresa Final.docx…]()


## **Objetivo**
* Construir un ETL (Extract, Transform, Load) en los servicios de AWS Glue.

## **Descripción**
* Este código lee desde el AWS Glue Datacatalog la tabla en una base de datos, después realiza una eliminacion de las columnas especificadas. Finalmente, escribe el resultado en un archivo Parquet en S3. 

## **Qué vamos a Aprender?** 
Al final del laboratorio el estudiante aprenderá:
* Como construir un ETL en AWS Glue y toda la configuración requerida para lograrlo.
* Como utilizar los rastreadores de AWS Glue para identificar estructuras de datos y alimentar el AWS Glue Data Catalog.
* Como leer información del AWS Glue Data Catalog y utilizarla para hacer las transformaciones necesarias.
* Como convertir archivos de CSV a Parquet.
* Como Cargar información transformada a Amazon S3.

## **Servicios de AWS a Utilizar**
* [Amazon Athena](https://aws.amazon.com/athena/).
* [AWS Glue](https://aws.amazon.com/glue/).
* [AWS Glue Data Catalog](https://docs.aws.amazon.com/es_es/glue/latest/dg/start-data-catalog.html).
* [AWS Glue Crawler](https://docs.aws.amazon.com/glue/latest/dg/add-crawler.html).
* [Amazon S3](https://aws.amazon.com/s3/).
* [IAM](https://aws.amazon.com/iam/).

## **Conceptos**
* **Dynamic Frame:** Es una estructura de datos similar a un DataFrame de Apache Spark, pero con características adicionales que lo hacen más adecuado para los procesos de transformación de datos en AWS Glue. 
* **Data Frame:** Es una estructura de datos similar a un DataFrame de Apache Spark, pero con características adicionales que lo hacen más adecuado para los procesos de transformación de datos en AWS Glue.
* **Inner Join:** es un tipo de operación de combinación (join) en bases de datos y sistemas de procesamiento de datos (como SQL, Spark, etc.) que se utiliza para combinar filas de dos tablas basándose en una condición común.

## **Explicación del Código**
A continuación voy a explicar detalladamente el código del archivo glue_dyf.py

## 1. Configuración inicial
```
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
```
- Importa librerías necesarias de Glue y PySpark.
- Glue corre sobre Spark, así que necesitas un SparkContext y un GlueContext.
```  
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
```
- Esto inicializa el Job de Glue y le da un contexto de ejecución.
## 2. Lectura de datos desde Glue Catalog
```
dyf_input = glueContext.create_dynamic_frame.from_catalog(
    database="db-prueba-1",
    table_name="myprimerintento_origen",
    transformation_ctx="dyf_input")
```
- Lee la tabla myprimerintento_origen de la base de Glue Catalog db-prueba-1.
- El resultado es un DynamicFrame, que es una estructura de Glue similar a un DataFrame de Spark, pero con ventajas para ETL.
## 3. Transformaciones
```
df_input = dyf_input.toDF()
```
- Lo pasamos a un DataFrame de Spark para usar funciones más avanzadas de transformación.
```
cols_to_drop = ["texto_1", "texto_2", "texto_3", "texto_4", "texto_5", "texto_6"]
df_clean = df_input.drop(*cols_to_drop)
```
- Se borran las columnas texto_1 a texto_6 del dataset.
```
df_partitioned = df_clean.withColumn("year", year(col("FECHA"))) \
                         .withColumn("month", month(col("FECHA"))) \
                         .withColumn("day", dayofmonth(col("FECHA")))
```
- Se extraen año, mes y día de la columna FECHA.
- Estas columnas nuevas (year, month, day) se usarán para particionar la salida en carpetas dentro de S3.
```
dyf_partitioned = DynamicFrame.fromDF(df_partitioned, glueContext, "dyf_partitioned")
```
- Glue escribe en S3 usando DynamicFrame, por eso convertimos de vuelta.
```
glueContext.write_dynamic_frame.from_options(
    frame=dyf_partitioned,
    connection_type="s3",
    connection_options={
        "path": "s3://myprimerintento-destino/clean-data/",
        "partitionKeys": ["year", "month", "day"]
    },
    format="parquet",
    transformation_ctx="write_output")
```
- Guarda los datos en el bucket myprimerintento-destino, carpeta clean-data/.
- Particiona automáticamente en subcarpetas por año, mes y día.
- El formato de salida es Parquet (más eficiente que CSV).
```
job.commit()
```
Marca el Job como completado para que Glue lo registre correctamente en la consola.
