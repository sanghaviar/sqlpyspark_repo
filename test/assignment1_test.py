import unittest
from src.Assignment1 import util
from pyspark.sql.types import StructField,StructType,IntegerType,StringType
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

class MyTestCase(unittest.TestCase):
    def test_something(self):
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
        # schema = ['name','dob','gender','salary']
        input_df = spark.createDataFrame(data, schema)


        data_out = [(6000,'03011998'),(40000,'10111998'),(6000,'02012000'),(22000,'03011998'),(200000,'04101998')]
        schema_out = ['salary','dob']
        output_df= spark.createDataFrame(data_out,schema_out)
        test_out = util.quest1(input_df)
        self.assertEqual(output_df.collect(), test_out.collect())  # add assertion here


if __name__ == '__main__':
    unittest.main()
