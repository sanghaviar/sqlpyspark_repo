import unittest
from src.Assignment1 import util
from pyspark.sql.types import StructField,StructType,IntegerType,StringType
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql.functions import col,lit,struct
def test_add_column(df):
    # Adding columns Country,Department,Age
    df1 = df.withColumn("Country",lit("").cast("string"))\
        .withColumn("Department",lit("").cast("string"))\
        .withColumn("Age",lit("").cast("string"))
    return df1

def test_changing_values(df1):
    # Changing the values of Salary column
    df2 = df1.withColumn("salary", col("salary") * 2)
    return df2

def test_change_dataType(df2):
    # Changing the data types of DOB and Salary to String
    df3 = df2.withColumn('dob',df2['dob'].cast('string'))\
       .withColumn('salary',df2['salary'].cast('string'))
    return df3

def test_derive_column(df3):
    # Derive a new column from salary column
    df4 = df3.withColumn('salary_increment',col('salary')*2)
    return df4

def test_rename_nested_column(df):
    new_schema = struct(col("name.firstname").alias("firstposition"),
                         col("name.middlename").alias("middleposition"),
                         col("name.lastname").alias("lastposition"))


    # Replace the "name" column with the new_schema
    df5  = df.withColumn("name", new_schema)
    return df5
def test_distict_column(df5):
    df6 = df5.withColumn('salary',df5['salary'].cast('Integer'))
    df6 = df6.select('salary','dob').distinct()
    return df6
def test_drop_column(df6):
    df7 = df6.drop('Department','age')
    return df7
schema = StructType([
            StructField("name", StructType([
                StructField("firstname", StringType()),
                StructField("middlename", StringType()),
                StructField("lastname", StringType())
            ])),
            StructField("dob", StringType()),
            StructField("gender", StringType()),
            StructField("salary", IntegerType())
        ])

data = [
            ({'firstname': 'james', 'middlename': '', 'lastname': 'smith'}, '03011998', 'M', 3000),
            ({'firstname': 'Michael', 'middlename': 'Rose', 'lastname': ''}, '10111998', 'M', 20000),
            ({'firstname': 'Robert', 'middlename': '', 'lastname': 'williams'}, '02012000', 'M', 3000),
            ({'firstname': 'Maria', 'middlename': 'Anne', 'lastname': 'jones'}, '03011998', 'F', 11000),
            ({'firstname': 'Jen', 'middlename': 'Mary', 'lastname': 'brown'}, '04101998', 'F', 100000)
        ]
df = spark.createDataFrame(data, schema)

fun1 = test_add_column(df)
fun2 = test_changing_values(fun1)
fun3 = test_change_dataType(fun2)
fun4 = test_derive_column(fun3)
fun5 = test_rename_nested_column(fun4)
fun6 = test_distict_column(fun5)
fun7 = test_drop_column(fun6)

class MyTestCase(unittest.TestCase):
    def test_case1(self):

        actual_output = util.add_column(df)
        expected_output = test_add_column(df)
        self.assertEqual(actual_output.collect(),expected_output.collect())
    def test_case2(self):

        actual_output = util.changing_values(fun1)
        expected_output = test_changing_values(fun1)
        self.assertEqual(actual_output.collect(),expected_output.collect())

    def test_case3(self):

        actual_output = util.change_dataType(fun2)
        expected_output = test_change_dataType(fun2)
        self.assertEqual(actual_output.collect(),expected_output.collect())
    def test_case4(self):

        actual_output = util.derive_column(fun3)
        expected_output = test_derive_column(fun3)
        self.assertEqual(actual_output.collect(),expected_output.collect())
    def test_case5(self):

        actual_output = util.rename_nested_column(fun4)
        expected_output = test_rename_nested_column(fun4)
        self.assertEqual(actual_output.collect(),expected_output.collect())

    def test_case6(self):

        actual_output = util.distict_column(fun5)
        expected_output = test_distict_column(fun5)
        self.assertEqual(actual_output.collect(),expected_output.collect())

    def test_case7(self):

        actual_output = util.drop_column(fun6)
        expected_output = test_drop_column(fun6)
        self.assertEqual(actual_output.collect(),expected_output.collect())


if __name__ == '__main__':
    unittest.main()
