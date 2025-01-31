# Data Engineering Coding Challenges

## Judgment Criteria

- Beauty of the code (beauty lies in the eyes of the beholder)
- Testing strategies
- Basic Engineering principles

## Problem 1

### Parse fixed width file

- Generate a fixed width file using the provided spec (offset provided in the spec file represent the length of each field).
- Implement a parser that can parse the fixed width file and generate a delimited file, like CSV for example.
- DO NOT use python libraries like pandas for parsing. You can use the standard library to write out a csv file (If you feel like)
- Language choices (Python or Scala)
- Deliver source via github or bitbucket
- Bonus points if you deliver a docker container (Dockerfile) that can be used to run the code (too lazy to install stuff that you might use)
- Pay attention to encoding

## Problem 2

### Data processing

- Generate a csv file containing first_name, last_name, address, date_of_birth
- Process the csv file to anonymise the data
- Columns to anonymise are first_name, last_name and address
- You might be thinking  that is silly
- Now make this work on 2GB csv file (should be doable on a laptop)
- Demonstrate that the same can work on bigger dataset
- Hint - You would need some distributed computing platform

## Choices

- Any language, any platform
- One of the above problems or both, if you feel like it.

How to Run Code :
## Install Dependencies:


### pip install -r requirements.txt

## Problem 1:

Input: tests/resources/input_problem1.txt

Expected Output: tests/resources/output_problem2.csv

## Problem2
Input: resources/problem2_input_file

Output: resources/problem2_output_file

## Testing:

pytest tests/

####  Note: A 2 GB file was required for Problem2, but GitHub restricts file sizes to 100 MB. Therefore, the file could not be pushed to GitHub. To work around this, I used the Faker library to generate 2 GB of fake data. 
#### Distributed Processing:  For processing large dataset, I utilized the PySpark library, which is designed to handle big data efficiently. 