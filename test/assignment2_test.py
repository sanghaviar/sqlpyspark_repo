import unittest
from src.Assignment2 import util
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

class MyTestCase(unittest.TestCase):
    def test_something(self):
        input_data = [('Banana', 1000, 'USA'), ('Carrots', 1500, 'INDIA'), ('Beans', 1600, 'Swedan'), ('Orange', 2000, 'UK'),
                ('Orange', 2000, 'UAE'), ('Banana', 400, 'China'), ('Carrots', 1200, 'China')]
        input_schema = ('Product', 'Amount', 'Country')
        df = spark.createDataFrame(input_data, input_schema)
        test_out = util.pivot_asgn(df)

        output_data = [('Orange','UAE',2000),('Orange','UK',2000),('Beans','Swedan',1600),('Banana','China',400),('Carrots','China',1200),('Carrots','INDIA',1500)]
        output_schema = ['Product','Country','Total']
        output_df = spark.createDataFrame(output_data,output_schema)
        self.assertEqual(output_df.collect(),test_out.collect())  # add assertion here


if __name__ == '__main__':
    unittest.main()
