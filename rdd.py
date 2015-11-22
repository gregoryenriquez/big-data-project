
import os
import urllib
import zipfile
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS
import math

sc = SparkContext()


datasets_path = os.path.join('/user/user01/spark_git/cs286project/', 'input')

# load the raw data for ratings
small_ratings_file = os.path.join(datasets_path, '', 'user_ratings_int_uid_int_itemid_full_11_15.csv')

# filter out the header
small_ratings_raw_data = sc.textFile(small_ratings_file)
#small_ratings_raw_data_header = small_ratings_raw_data.take(1)[0]

# parse the raw data into a new rdd
small_ratings_data = small_ratings_raw_data.map(lambda line: line.split(",")).map(lambda tokens: (tokens[0],tokens[1],tokens[2])).cache()

#first three lines
print small_ratings_data.take(3)

# do the same thing on item file
small_movies_file = os.path.join(datasets_path, '', 'id_title_int_itemid_str_title_full_11_15.csv')

# filter header if needed
small_movies_raw_data = sc.textFile(small_movies_file)
#small_movies_raw_data_header = small_movies_raw_data.take(1)[0]

# parse the raw data into a new rdd
small_movies_data = small_movies_raw_data.map(lambda line: line.split(",")).map(lambda tokens: (tokens[0], tokens[1])).cache()

# print frirst three lines
print small_movies_data.take(3)

#print small_ratings_raw_data
print "hello worldd"

# dividing the rating dataset into 3 parts for training, validation and test.
training_RDD, validation_RDD, test_RDD = small_ratings_data.randomSplit([6, 2, 2],seed=0L)
validation_for_predict_RDD = validation_RDD.map(lambda x: (x[0], x[1]))
test_for_predict_RDD = test_RDD.map(lambda x: (x[0], x[1]))

# starting training phase
seed = 5L
iterations = 10
regularization_parameter = 0.1
ranks = [4, 8, 12]
errors = [0, 0, 0]
err = 0
tolerance = 0.02

min_error = float('inf')
best_rank = -1
best_iteration = -1
for rank in ranks:
	model = ALS.train(training_RDD, rank, seed=seed, iterations=iterations, lambda_=regularization_parameter)
	predictions = model.predictAll(validation_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
	rates_and_preds = validation_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
	error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
	errors[err] = error
	err += 1
	print 'For rank %s the RMSE is %s' % (rank, error)
	if error < min_error:
        	min_error = error
        	best_rank = rank

print 'The best model was trained with rank %s' % best_rank

print predictions.take(3)
print rates_and_preds.take(3)

model = ALS.train(training_RDD, best_rank, seed=seed, iterations=iterations,lambda_=regularization_parameter)
predictions = model.predictAll(test_for_predict_RDD).map(lambda r: ((r[0], r[1]), r[2]))
rates_and_preds = test_RDD.map(lambda r: ((int(r[0]), int(r[1])), float(r[2]))).join(predictions)
error = math.sqrt(rates_and_preds.map(lambda r: (r[1][0] - r[1][1])**2).mean())

print 'For testing data the RMSE is %s' % (error)
