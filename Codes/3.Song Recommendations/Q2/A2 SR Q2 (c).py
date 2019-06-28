
from pyspark.mllib.recommendation import Rating
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics



# Get predicted ratings on all existing user-product pairs

testData = ratings.map(lambda p: (p.user, p.product))
model.recommendForAllUsers()
predictions = model.predictAll(testData).map(lambda r: ((r.user, r.product), r.rating))

ratingsTuple = ratings.map(lambda r: ((r.user, r.product), r.rating))
predictionAndLabels = predictions.join(ratingsTuple).map(lambda tup: tup[1])




# calculate 

 metrics = RankingMetrics(predictionAndLabels)

 metrics.precisionAt(5)

 metrics.ndcgAt(10)
 
 metrics.meanAveragePrecision

 











