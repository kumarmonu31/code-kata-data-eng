import unittest
import json
import os
import tempfile
import csv
from Problem1 import Problem1


class TestProblem1(unittest.TestCase):
    def setUp(self):

        self.test_dir = tempfile.mkdtemp()
        self.spec_file = os.path.join(self.test_dir, 'spec.json')
        self.test_data_file = os.path.join(self.test_dir, 'input_problem1.txt')
        self.output_file = os.path.join(self.test_dir, 'output.csv')

        self.spec = {
            "ColumnNames": [
                "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10"
            ],
            "Offsets": [
                "5", "12", "3", "2", "13", "7", "10", "13", "20", "13"
            ],
            "FixedWidthEncoding": "windows-1252",
            "IncludeHeader": "True",
            "DelimitedEncoding": "utf-8"
        }

        with open(self.spec_file, 'w') as f:
            json.dump(self.spec, f, indent=4)

        self.test_data = """\
00001MONU KUMAR   M 27HY DELHI IND 0000001 Developer 555-111-0001john.smith@email.com Python
00001RAHUL SINGH  M 28PATNA  BIHAR 0000001 Developer 555-111-0001john.smith@email.com JAVA
"""
        with open(self.test_data_file, 'w', encoding='windows-1252') as f:
            f.write(self.test_data)

    def tearDown(self):
        for file in [self.spec_file, self.test_data_file, self.output_file]:
            if os.path.exists(file):
                os.remove(file)
        os.rmdir(self.test_dir)

    def test_conversion(self):
        # Run the conversion
        problem = Problem1(self.spec_file)
        problem.convert_fixed_width_to_csv(self.test_data_file, self.output_file)
        expected_output = """f1,f2,f3,f4,f5,f6,f7,f8,f9,f10
00001,MONU KUMAR,M,27,HY DELHI IND,0000001,Developer,555-111-0001,john.smith@email.com,Python
00001,RAHUL SINGH,M,28,PATNA  BIHAR,0000001,Developer,555-111-0001,john.smith@email.com,JAVA
"""

        with open(self.output_file, 'r', encoding='utf-8') as f:
            output = f.read()

        self.assertEqual(output, expected_output)

if __name__ == '__main__':
    unittest.main()
