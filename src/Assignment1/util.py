from pyspark.sql.functions import col,lit,struct,min,max
def quest1(df):
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
    df = df.withColumn('salary',df['salary'].cast('bigint'))
    df = df.select('salary','dob').distinct()
    df = df.drop('Department','age')
    return df
    #df.show()


