import unittest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from Problem2 import Problem2

class TestProblem2(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("Test Data Anonymization") \
            .master("local[*]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):

        self.data = [
            Row(first_name="John", last_name="Doe", address="123 Elm Street"),
            Row(first_name="Jane", last_name="Smith", address="456 Oak Street")
        ]
        self.df = self.spark.createDataFrame(self.data)
        self.input_file = "test_input.csv"
        self.output_file = "test_output.csv"
        self.df.write.csv(self.input_file, header=True, mode='overwrite')

    def test_md5_hash(self):
        anonymizer = Problem2(self.input_file, self.output_file)
        hash_value = anonymizer.md5_hash("Jane")
        self.assertEqual(hash_value, "2b95993380f8be6bd4bd46bf44f98db9")

    def test_anonymize_column(self):
        anonymizer = Problem2(self.input_file, self.output_file)
        anonymizer.df = self.df
        anonymizer.anonymize_column('address')

        anonymized_df = anonymizer.df.collect()
        for row in anonymized_df:
            self.assertNotEqual(row.address, "123 Elm Street")
            self.assertNotEqual(row.address, "456 Oak Street")

    def test_process_data(self):
        anonymizer = Problem2(self.input_file, self.output_file)
        anonymizer.process_data()

        anonymized_df = self.spark.read.csv(self.output_file, header=True).collect()
        for row in anonymized_df:
            self.assertNotEqual(row.first_name, "John")
            self.assertNotEqual(row.first_name, "Jane")

if __name__ == "__main__":
    unittest.main()
