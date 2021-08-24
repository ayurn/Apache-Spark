Spark version: 3.1.2
Loading data from Pyspark to mysql
save dataframe to hdfs as csv

user_counts.repartition(1).write.save('hdfs://localhost:9000/sparkData/MySql_operations', format='csv', mode='append')
Read csv file from hdfs to pyspark dataframe
Step 1:
Create schema for dataframe

schema = StructType([
StructField("user_name", StringType(), True),
StructField("count", IntegerType(), True)])
Step 2:

count_df = spark.read.format("csv").option("header", "false").schema(schema).load("hdfs://localhost:9000/sparkData/MySql_operations/part-00000-ef648781-639e-4993-9528-42795999c2f8-c000.csv")
Save pyspark dataframe to mysql as a table

jdbc_url = f"jdbc:mysql://localhost:3306/"+ {database}

count_df.write.format('jdbc').option("url",jdbc_url) \
    .mode("overwrite") \
    .option("dbtable","total_user_count") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("user",user).option("password",password).save()
    


Load sql table as a dataframe in pyspark
df_COUNT = spark.read.format("jdbc") \
    .option("url","jdbc:mysql://localhost:3306/sparkOperations") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("dbtable","total_user_count") \
    .option("user",user) \
    .option("password",password).load()