

#### Logistics Regression
# Metrics

total = pred1.count()
nP = pred1.filter((F.col('prediction') == 1)).count()
nN = pred1.filter((F.col('prediction') == 0)).count()
TP = pred1.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 1)).count()
FP = pred1.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 0)).count()
FN = pred1.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 1)).count()
TN = pred1.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 0)).count()

print('num positive: {}'.format(nP))
print('num negative: {}'.format(nN))
print('precision: {}'.format(TP / (TP + FP)))
print('recall: {}'.format(TP / (TP + FN)))
print('accuracy: {}'.format((TP + TN) / total))

#um positive: 36254
#um negative: 28357
#recision: 0.5920174325591658
#ecall: 0.6623156205640931
#ccuracy: 0.6017086873752148


 ------------------------------------------------------------------------------------------

#### Navie Bayes
# Metrics
total = pred2.count()
nP = pred2.filter((F.col('prediction') == 1)).count()
nN = pred2.filter((F.col('prediction') == 0)).count()
TP = pred2.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 1)).count()
FP = pred2.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 0)).count()
FN = pred2.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 1)).count()
TN = pred2.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 0)).count()

print('num positive: {}'.format(nP))
print('num negative: {}'.format(nN))
print('precision: {}'.format(TP / (TP + FP)))
print('recall: {}'.format(TP / (TP + FN)))
print('accuracy: {}'.format((TP + TN) / total))


#um positive: 27297
#um negative: 37314
#recision: 0.4982965161006704
#ecall: 0.4197370857248658
#ccuracy: 0.49700515392115896



--------------------------------------------------------------------------------------------------

#### Random forest
# Metrics
total = pred3.count()
nP = pred3.filter((F.col('prediction') == 1)).count()
nN = pred3.filter((F.col('prediction') == 0)).count()
TP = pred3.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 1)).count()
FP = pred3.filter((F.col('prediction') == 1) & (F.col('is_electronic') == 0)).count()
FN = pred3.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 1)).count()
TN = pred3.filter((F.col('prediction') == 0) & (F.col('is_electronic') == 0)).count()

print('num positive: {}'.format(nP))
print('num negative: {}'.format(nN))
print('precision: {}'.format(TP / (TP + FP)))
print('recall: {}'.format(TP / (TP + FN)))
print('accuracy: {}'.format((TP + TN) / total))


#um positive: 29158
#um negative: 35453
#recision: 0.6427738528019754
#ecall: 0.5783496883293218
#ccuracy: 0.6273080435220009












