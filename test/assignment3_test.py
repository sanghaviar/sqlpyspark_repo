import unittest
from src.Asssignment3 import util
from pyspark.sql import SparkSession
from pyspark.sql.functions import first,col,max,min,avg,sum,desc

def test_first_row(df):
    # Select first row from each department group.
    df1 = df.groupby('department').agg(first('employee_name').alias('employee_name'), first('salary').alias('salary'))
    return df1

def test_highest_salary(df):
    # Employee who earns highest salary
    df2 = df.orderBy(col('salary').desc()).limit(1)
    return df2

def test_agg_fun(df,df1,df2):
    # Select the highest, lowest, average, and total salary for each department group.
    df3 = df.groupBy('department').agg(max('salary').alias('Maximum Salary'))
    df4 = df.groupby('department').agg(min('salary').alias('Minimum Salary'))
    df5 = df.groupby('department').agg(avg('salary').alias('Average salary'))
    df6 = df.groupby('department').agg(sum('salary').alias('Total salary'))

    combined_df = df1.join(df2, on='department', how="outer") \
        .join(df3, on="department", how="outer") \
        .join(df4, on="department", how="outer") \
        .join(df5, on="department", how='outer') \
        .join(df6, on="department", how='outer')
    # combined_df.show(truncate = False)
    return combined_df

spark = SparkSession.builder.getOrCreate()
data = [('Olivia', 'IT', 3800),
        ('Liam', 'IT', 5300),
        ('Emma', 'IT', 4900),
        ('Noah', 'HR', 4100),
        ('Ava', 'HR', 3600),
        ('Ethan', 'HR', 4200),
        ('Sophia', 'HR', 4500),
        ('Mia', 'Marketing', 4300),
        ('Lucas', 'Marketing', 2400)]

schema = ['employee_name', 'department', 'salary']
df = spark.createDataFrame(data, schema)
fun1 = test_first_row(df)
fun2 = test_highest_salary(df)
fun3 = test_agg_fun(df,fun1,fun2)
class MyTestCase(unittest.TestCase):
    def test_case1(self):
        actual_output = util.first_row(df)
        expected_output = test_first_row(df)
        self.assertEqual(actual_output.collect(),expected_output.collect())
    def test_case2(self):
        actual_output = util.highest_salary(df)
        expected_output = test_highest_salary(df)
        self.assertEqual(actual_output.collect(),expected_output.collect())

    def test_case3(self):
        actual_output = util.agg_fun(df,fun1,fun2)
        expected_output = test_agg_fun(df,fun1,fun2)
        self.assertEqual(actual_output.collect(),expected_output.collect())


if __name__ == '__main__':
    unittest.main()
