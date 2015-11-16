import os
from pyspark import SparkContext

print("test")

sc = SparkContext(appName="PythonALS")

datasets_path = os.getcwd()

small_ratings_file = os.path.join(datasets_path, 'input', 'users_sample.csv')

small_ratings_raw_data = sc.textFile(small_ratings_file)
small_ratings_raw_data_header = small_ratings_raw_data.take(1)[0]

small_ratings_data = small_ratings_raw_data.filter(lambda line: line!=small_ratings_raw_data_header)\
  .map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1], tokens[2])).cache()

print(small_ratings_data.take(3))

