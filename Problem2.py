from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import hashlib


class Problem2:
    def __init__(self, input_file: str, output_file: str):
        """
        Initialize the Problem2 with input and output file paths.

        :param input_file: Path to the input CSV file
        :param output_file: Path to the output CSV file
        """
        self.input_file = input_file
        self.output_file = output_file
        self.spark = SparkSession.builder \
            .appName("Data Anonymization") \
            .getOrCreate()

    @staticmethod
    def md5_hash(value: str) -> str:
        """
        Hash the given value using MD5.

        :param value: String value to hash
        :return: MD5 hashed value
        """
        if value is not None:
            return hashlib.md5(value.encode('utf-8')).hexdigest()
        return None

    def anonymize_column(self, column_name: str) -> None:
        """
        Anonymize a specified column in the DataFrame using MD5 hashing.

        :param column_name: Name of the column to anonymize
        """
        hash_udf = udf(self.md5_hash, StringType())
        self.df = self.df.withColumn(column_name, hash_udf(col(column_name)))

    def process_data(self) -> None:
        """
        Read the CSV file, anonymize specified columns, and write the result to a new CSV file.
        """
        # Read the CSV file into a Spark DataFrame
        self.df = self.spark.read.csv(self.input_file, header=True, inferSchema=True)

        # Anonymize specified columns
        self.anonymize_column('first_name')
        self.anonymize_column('last_name')
        self.anonymize_column('address')

        # Save the anonymized DataFrame to a new CSV file
        self.df.write.csv(self.output_file, header=True)

    def stop(self) -> None:
        """
        Stop the Spark session.
        """
        self.spark.stop()



# Example usage:
if __name__ == "__main__":
    input_file = "resources/problem2_input_file.csv"
    output_file = "resources/problem2_output_file.csv"

    anonymizer = Problem2(input_file, output_file)
    anonymizer.process_data()
    anonymizer.stop()
