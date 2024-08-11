import csv
from faker import Faker


class CSVGeneratorUtils:
    @staticmethod
    def generate_large_csv(output_file, target_size_mb, chunk_size=100000):
        """
        Generate a large CSV file of approximately the specified size.
        """
        fake = Faker()
        rows_per_chunk = chunk_size
        target_size_bytes = target_size_mb * 1024 * 1024
        estimated_row_size = 500  # Estimated size of each row in bytes
        num_chunks = (target_size_bytes // (rows_per_chunk * estimated_row_size)) + 1

        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['first_name', 'last_name', 'address', 'date_of_birth']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for _ in range(num_chunks):
                CSVGeneratorUtils.write_chunk(writer, rows_per_chunk, fake)

    @staticmethod
    def write_chunk(writer, rows_per_chunk, fake):
        """
        Write a chunk of data to the CSV file.
        """
        for _ in range(rows_per_chunk):
            writer.writerow({
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'address': fake.address().replace('\n', ', '),
                'date_of_birth': fake.date_of_birth().strftime('%Y-%m-%d')
            })


if __name__ == "__main__":
    output_file = '/Users/monu.kumar/PycharmProjects/Demo/StockData/resources/problem2_input_file.csv'
    target_size_mb = 2048  # 2GB in megabytes

    CSVGeneratorUtils.generate_large_csv(output_file, target_size_mb)
    print(f"CSV file '{output_file}' with approximately {target_size_mb}MB generated successfully.")
