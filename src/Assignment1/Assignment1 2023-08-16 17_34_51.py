# Databricks notebook source
from pyspark.sql.functions import col,lit,struct,min,max
from pyspark.sql.types import StructField,StructType,IntegerType,StringType

schema = StructType([
    StructField("name", StructType([
        StructField("firstname", StringType()),
        StructField("middlename", StringType()),
        StructField("lastname", StringType())
        ])),
    StructField("dob", StringType()),
    StructField("gender",StringType()),
    StructField("salary",IntegerType())    
])

data = [
    ({'firstname':'james','middlename':'','lastname':'smith'},'03011998','M',3000),
    ({'firstname': 'Michael', 'middlename': 'Rose', 'lastname': ''},'10111998','M',20000),
    ({'firstname': 'Robert', 'middlename': '', 'lastname': 'williams'},'02012000','M',3000),
    ({'firstname': 'Maria', 'middlename': 'Anne', 'lastname': 'jones'},'03011998','F',11000),
    ({'firstname': 'Jen', 'middlename': 'Mary', 'lastname': 'brown'},'04101998','F',100000)
]
# schema = ['name','dob','gender','salary']
df = spark.createDataFrame(data,schema)

def person1(df):
    # Adding columns Country,Department,Age
    df = df.withColumn("Country",lit("").cast("string"))\
        .withColumn("Department",lit("").cast("string"))\
        .withColumn("Age",lit("").cast("string"))

    # Changing the values of Salary column 
    df = df.withColumn("salary", col("salary") * 2)

    # Changing the data types of DOB and Salary to String 
    df = df.withColumn('dob',df['dob'].cast('string'))\
       .withColumn('salary',df['salary'].cast('string'))

    # Derive a new column from salary column 
    df = df.withColumn('salary_increment',col('salary')*2)

    new_schema = struct(col("name.firstname").alias("firstposition"),
                         col("name.middlename").alias("middleposition"),
                         col("name.lastname").alias("lastposition"))
    # Replace the "name" column with the new_schema
    df  = df.withColumn("name", new_schema)

    df = df.withColumn('salary',df['salary'].cast('Integer'))
    # df = df.groupBy('name.firstposition').agg(max(col('salary')))
    # df = df.agg(max(col('salary'))).groupBy('name.firstposition')
    # df =  df.select(max(col('salary')))
    # sal = df.filter(col('salary')== max_salary).select(name.firstposition).first()[0]
    # print(df)
    df = df.select('salary','dob').distinct()
    df = df.drop('Department','age')
    display(df)

person1(df)


