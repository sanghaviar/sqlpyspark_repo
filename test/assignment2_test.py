import unittest
from pyspark.sql.functions import col,expr
from src.Assignment2 import util
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def test_pivot_asgn(df):
    pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
    unpivotexp = "stack(5,'China',China,'INDIA',INDIA,'Swedan',Swedan,'UAE',UAE,'UK',UK) as (Country,Total)"

    unPivotDF = pivotDF.select("Product", expr(unpivotexp)) \
        .where("Total is not null")
    #unPivotDF.show(truncate=False)
    return unPivotDF

class MyTestCase(unittest.TestCase):
    def test_case1(self):
        data = [('Banana', 1000, 'USA'),
                ('Carrots', 1500, 'INDIA'),
                ('Beans', 1600, 'Swedan'),
                ('Orange', 2000, 'UK'),
                ('Orange', 2000, 'UAE'),
                ('Banana', 400, 'China'),
                ('Carrots', 1200, 'China')]
        schema = ('Product', 'Amount', 'Country')
        df = spark.createDataFrame(data, schema)

        x = util.pivot_asgn(df)
        expected_output = test_pivot_asgn(df)

        self.assertEqual(x.collect(),expected_output.collect())


    def test_case2(self):
        data = [('Rice', 2000, 'USA'),
                ('Sugar', 1500, 'INDIA'),
                ('Ragi', 1600, 'Swedan'),
                ('Wheat', 2000, 'UK'),
                ('Bajra',3000, 'UAE'),
                ('Millets',9000, 'China'),
                ('Jowar', 1900, 'China')]
        schema = ('Product', 'Amount', 'Country')
        df = spark.createDataFrame(data, schema)

        x = util.pivot_asgn(df)
        expected_output = test_pivot_asgn(df)

        self.assertEqual(x.collect(),expected_output.collect())

    def test_case3(self):
        data = [('Carrot', 2000, 'USA'),
                ('Radish', 1500, 'INDIA'),
                ('Beetroot', 1600, 'Swedan'),
                ('Tomato', 2000, 'UK'),
                ('Potato', 3000, 'UAE'),
                ('Brinjal', 9000, 'China'),
                ('Beans', 1900, 'China')]
        schema = ('Product', 'Amount', 'Country')
        df = spark.createDataFrame(data, schema)

        x = util.pivot_asgn(df)
        expected_output = test_pivot_asgn(df)

        self.assertEqual(x.collect(), expected_output.collect())




if __name__ == '__main__':
    unittest.main()
