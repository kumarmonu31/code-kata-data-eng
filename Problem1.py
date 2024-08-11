import csv
import json
from typing import List

class Problem1:
    def __init__(self, spec_file):
        self.spec_file = spec_file
        self.load_spec()

    def load_spec(self):
        # Load the spec from the JSON file
        with open(self.spec_file, 'r') as f:
            spec = json.load(f)

        # Extract relevant fields from the spec
        self.column_names = spec['ColumnNames']
        self.offsets = list(map(int, spec['Offsets']))  # Convert offsets to integers
        self.fixed_width_encoding = spec['FixedWidthEncoding']
        self.include_header = spec['IncludeHeader'].lower() == 'true'
        self.delimited_encoding = spec['DelimitedEncoding']

    def parse_fixed_width(self, line: str) -> List[str]:
        """
            Parse a single line of fixed-width formatted data into individual fields based on predefined offsets.

            Parameters:
            - line (str): A line of text from the fixed-width formatted file.

            Returns:
            - list of str: A list of fields extracted from the line, each field corresponding to a fixed-width segment defined by `self.offsets`.
            """
        start = 0
        fields = []
        for offset in self.offsets:
            end = start + offset
            field = line[start:end].strip()
            fields.append(field)
            start = end
        return fields

    def convert_fixed_width_to_csv(self, input_file: str, output_file: str) -> None:
        """
            Convert a fixed-width formatted file to a CSV file based on the provided specification.

            Parameters:
            - input_file (str): Path to the input file containing data in fixed-width format.
            - output_file (str): Path to the output CSV file where the converted data will be saved.
            """
        with open(input_file, 'r', encoding=self.fixed_width_encoding) as infile, \
                open(output_file, 'w', newline='', encoding=self.delimited_encoding) as outfile:

            writer = csv.writer(outfile)
            if self.include_header:
                writer.writerow(self.column_names)
            for line in infile:
                parsed_fields = self.parse_fixed_width(line)
                writer.writerow(parsed_fields)

# Example usage:
if __name__ == "__main__":
    spec_file = r'resources/spec.json'
    input_file = r"tests/resources/input_problem1.txt"
    output_file = r"/Users/monu.kumar/PycharmProjects/Demo/StockData/tests/resources/output_problem2.csv"

    problem = Problem1(spec_file)
    problem.convert_fixed_width_to_csv(input_file, output_file)
