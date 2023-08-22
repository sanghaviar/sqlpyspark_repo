import unittest
from pyspark.sql.functions import col,expr
from src.Assignment2 import util
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def test_pivot_fun(df):
    pivotDF = df.groupBy("Product").pivot("Country").sum("Amount")
    # pivotDF.show()
    return pivotDF

def test_unpivoit_fun(pivotDF):
    unpivotexp = "stack(5,'China',China,'INDIA',INDIA,'Swedan',Swedan,'UAE',UAE,'UK',UK) as (Country,Total)"

    unPivotDF = pivotDF.select("Product", expr(unpivotexp)) \
        .where("Total is not null")
    # unPivotDF.show(truncate=False)
    return unPivotDF


data =  [('Rice', 2000, 'USA'),
        ('Sugar', 1500, 'INDIA'),
        ('Ragi', 1600, 'Swedan'),
        ('Wheat', 2000, 'UK'),
        ('Bajra',3000, 'UAE'),
        ('Millets',9000, 'China'),
        ('Jowar', 1900, 'China')]
schema = ('Product', 'Amount', 'Country')
df = spark.createDataFrame(data, schema)

fun1 = test_pivot_fun(df)
fun2 = test_unpivoit_fun(fun1)
class MyTestCase(unittest.TestCase):
    def test_case1(self):

        actual_output  = util.pivot_fun(df)
        expected_output = test_pivot_fun(df)
        self.assertEqual(actual_output.collect(),expected_output.collect())

    def test_case2(self):
        actual_output = util.unpivoit_fun(fun1)
        expected_output = test_unpivoit_fun(fun1)
        self.assertEqual(actual_output.collect(), expected_output.collect())





if __name__ == '__main__':
    unittest.main()
