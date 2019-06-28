#### cross validation


#create a label column to fit the model 
traing = traing.withColumn(
	"label",
	 traing.is_electronic
	  )

test = test.withColumn(
	"label",
	 test.is_electronic
	  )

#----------------------------------------------------------------------------------
#### Logistics Regression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


# Create 5-fold CrossValidator
lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0,featuresCol="features", labelCol='is_electronic')
grid = ParamGridBuilder().addGrid(lr.maxIter, [0, 1]).build()
evaluator = BinaryClassificationEvaluator()
cv = CrossValidator(estimator=lr, estimatorParamMaps=grid, evaluator=evaluator,numFolds=5)
cvModel = cv.fit(traing)
pred4 = cvModel.transform(test)


# Metrics
total = pred4.count()
nP = pred4.filter((F.col('prediction') == 1)).count()
nN = pred4.filter((F.col('prediction') == 0)).count()
TP = pred4.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 1)).count()
FP = pred4.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 0)).count()
FN = pred4.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 1)).count()
TN = pred4.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 0)).count()
print('num positive: {}'.format(nP))
print('num negative: {}'.format(nN))
print('precision: {}'.format(TP / (TP + FP)))
print('recall: {}'.format(TP / (TP + FN)))
print('accuracy: {}'.format((TP + TN) / total))

#   num positive: 64433
#   num negative: 178
#   precision: 0.5014045597752704
#   recall: 0.9969450101832994
#   accuracy: 0.5012459178777607



#----------------------------------------------------------------------------
#### NaiveBayes
nb = NaiveBayes(smoothing=1,featuresCol="features", labelCol='is_electronic')
grid = ParamGridBuilder().build()
evaluator = BinaryClassificationEvaluator()
cv = CrossValidator(estimator=nb, estimatorParamMaps=grid, evaluator=evaluator,numFolds=5)
cvModel = cv.fit(traing)
pred5 = cvModel.transform(test)


# Metrics
total = pred5.count()
nP = pred5.filter((F.col('prediction') == 1)).count()
nN = pred5.filter((F.col('prediction') == 0)).count()
TP = pred5.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 1)).count()
FP = pred5.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 0)).count()
FN = pred5.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 1)).count()
TN = pred5.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 0)).count()
print('num positive: {}'.format(nP))
print('num negative: {}'.format(nN))
print('precision: {}'.format(TP / (TP + FP)))
print('recall: {}'.format(TP / (TP + FN)))
print('accuracy: {}'.format((TP + TN) / total))

#    num positive: 27297
#    num negative: 37314
#    precision: 0.4982965161006704
#    recall: 0.4197370857248658
#    accuracy: 0.49700515392115896

  

#----------------------------------------------------------------------------
#### Random Forest
rf = RandomForestClassifier(labelCol="is_electronic",
                             featuresCol="features",
                             numTrees = 100,
                             maxDepth = 4,
                            maxBins = 32)
grid = ParamGridBuilder().build()
evaluator = BinaryClassificationEvaluator()
cv = CrossValidator(estimator=rf, estimatorParamMaps=grid, evaluator=evaluator,numFolds=5)
cvModel = cv.fit(traing)
pred6 = cvModel.transform(test)


# Metrics
total = pred6.count()
nP = pred6.filter((F.col('prediction') == 1)).count()
nN = pred6.filter((F.col('prediction') == 0)).count()
TP = pred6.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 1)).count()
FP = pred6.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 0)).count()
FN = pred6.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 1)).count()
TN = pred6.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 0)).count()
print('num positive: {}'.format(nP))
print('num negative: {}'.format(nN))
print('precision: {}'.format(TP / (TP + FP)))
print('recall: {}'.format(TP / (TP + FN)))
print('accuracy: {}'.format((TP + TN) / total))

   
   
#    num positive: 29158
#    num negative: 35453
#    precision: 0.6427738528019754
#    recall: 0.5783496883293218
#    accuracy: 0.6273080435220009










































