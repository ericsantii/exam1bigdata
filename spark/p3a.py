from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession.builder.getOrCreate()
schemaesc = StructType([
    StructField("region_educativa_escuela", StringType()),
    StructField("distrito_escolar_escuela", StringType()),
    StructField("ciudad", StringType()),
    StructField("id", IntegerType()),
    StructField("nombre_escuela", StringType()),
    StructField("nivel_escuela", StringType()),
    StructField("numero_serie_college", StringType())
])
school_data_frame = spark.read.csv(
    "/user/escuelasPR.csv", header=False, schema=schemaesc)

schemastud = StructType([
    StructField("region_educativa_escuela", StringType()),
    StructField("distrito_escolar_escuela", StringType()),
    StructField("id", IntegerType()),
    StructField("nombre_escuela", StringType()),
    StructField("nivel_escuela", StringType()),
    StructField("sexo", StringType()),
    StructField("id_estudiante", IntegerType())])
student_data_frame = spark.read.csv(
    "/user/studentsPR.csv", header=False, schema=schemastud)

student_data_frame.join(school_data_frame, 'id').select(student_data_frame.id_estudiante).filter((school_data_frame.nivel_escuela == 'Superior') & (
    (school_data_frame.ciudad == 'Ponce') | (school_data_frame.ciudad == 'San Juan'))).write.csv('/user/p3aresults')
