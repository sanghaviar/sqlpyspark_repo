from pyspark.sql.functions import col,lit,struct,min,max
def add_column(df):
    # Adding columns Country,Department,Age
    df1 = df.withColumn("Country",lit("").cast("string"))\
        .withColumn("Department",lit("").cast("string"))\
        .withColumn("Age",lit("").cast("string"))
    return df1

def changing_values(df1):
    # Changing the values of Salary column
    df2 = df1.withColumn("salary", col("salary") * 2)
    return df2

def change_dataType(df2):
    # Changing the data types of DOB and Salary to String
    df3 = df2.withColumn('dob',df2['dob'].cast('string'))\
       .withColumn('salary',df2['salary'].cast('string'))
    return df3

def derive_column(df3):
    # Derive a new column from salary column
    df4 = df3.withColumn('salary_increment',col('salary')*2)
    return df4

def rename_nested_column(df):
    new_schema = struct(col("name.firstname").alias("firstposition"),
                         col("name.middlename").alias("middleposition"),
                         col("name.lastname").alias("lastposition"))


    # Replace the "name" column with the new_schema
    df5  = df.withColumn("name", new_schema)
    return df5
def distict_column(df5):
    df6 = df5.withColumn('salary',df5['salary'].cast('Integer'))
    df6 = df6.select('salary','dob').distinct()
    return df6
def drop_column(df6):
    df7 = df6.drop('Department','age')
    return df7
    #df.show()


