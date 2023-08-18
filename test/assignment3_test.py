import unittest
from src.Asssignment3 import util
from pyspark.sql import SparkSession
from pyspark.sql.functions import first,col,max,min,avg,sum,desc

def test_ques3(df):
    df1 = df.groupby('department').agg(first('employee_name').alias('employee_name'), first('salary').alias('salary'))

    df2 = df.orderBy(col('salary').desc()).limit(1)

    df3 = df.groupBy('department').agg(max('salary').alias('Maximum Salary'))
    df4 = df.groupby('department').agg(min('salary').alias('Minimum Salary'))
    df5 = df.groupby('department').agg(avg('salary').alias('Average salary'))
    df6 = df.groupby('department').agg(sum('salary').alias('Total salary'))

    combined_df = df1.join(df2, on='department', how="outer") \
        .join(df3, on="department", how="outer") \
        .join(df4, on="department", how="outer") \
        .join(df5, on="department", how='outer') \
        .join(df6, on="department", how='outer')
    return combined_df




class MyTestCase(unittest.TestCase):
    def test_case1(self):

        spark = SparkSession.builder.getOrCreate()
        data = [('James', 'Sales', 3000),
                ('Michael', 'Sales', 4600),
                ('Robert', 'Sales', 4100),
                ('Maria', 'Finance', 3000),
                ('Roman', 'Finance', 3000),
                ('Scott', 'Finance', 3300),
                ('Jen', 'Finance', 3900),
                ('Jeff', 'Marketing', 3000),
                ('Kumar', 'Marketing', 2000)]
        schema = ['employee_name', 'department', 'salary']
        df = spark.createDataFrame(data, schema)
        x = util.ques3(df)
        expected_output = test_ques3(df)
        self.assertEqual(x.collect(),expected_output.collect())

    def test_case2(self):

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
        x = util.ques3(df)
        expected_output = test_ques3(df)
        self.assertEqual(x.collect(),expected_output.collect())

    def test_case3(self):

        spark = SparkSession.builder.getOrCreate()
        data = [('Jennifer', 'Engineering', 3500),
                ('Matthew', 'Engineering', 4200),
                ('Emily', 'Engineering', 4000),
                ('Daniel', 'Research', 2800),
                ('Oliver', 'Research', 3200),
                ('Sophia', 'Research', 3600),
                ('William', 'Marketing', 4100),
                ('Jessica', 'Marketing', 2800),
                ('Aiden', 'Marketing', 2500)]

        schema = ['employee_name', 'department', 'salary']
        df = spark.createDataFrame(data, schema)
        x = util.ques3(df)
        expected_output = test_ques3(df)
        self.assertEqual(x.collect(),expected_output.collect())

if __name__ == '__main__':
    unittest.main()
