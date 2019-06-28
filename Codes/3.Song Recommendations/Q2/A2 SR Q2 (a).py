


# ALS
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

import numpy as np
import scipy as sp

# Build the recommendation model using ALS on the training data
als = ALS(userCol="userID1", itemCol="songID1", ratingCol="play_count")
model = als.fit(training_data)

# model.structure:

dir(model)

#     
#     ['__class__',
#      '__del__',
#      '__delattr__',
#      '__dict__',
#      '__dir__',
#      '__doc__',
#      '__eq__',
#      '__format__',
#      '__ge__',
#      '__getattribute__',
#      '__gt__',
#      '__hash__',
#      '__init__',
#      '__init_subclass__',
#      '__le__',
#      '__lt__',
#      '__metaclass__',
#      '__module__',
#      '__ne__',
#      '__new__',
#      '__reduce__',
#      '__reduce_ex__',
#      '__repr__',
#      '__setattr__',
#      '__sizeof__',
#      '__str__',
#      '__subclasshook__',
#      '__weakref__',
#      '_call_java',
#      '_clear',
#      '_copyValues',
#      '_copy_params',
#      '_create_from_java_class',
#      '_create_params_from_java',
#      '_defaultParamMap',
#      '_dummy',
#      '_empty_java_param_map',
#      '_from_java',
#      '_java_obj',
#      '_make_java_param_pair',
#      '_new_java_array',
#      '_new_java_obj',
#      '_paramMap',
#      '_params',
#      '_randomUID',
#      '_resetUid',
#      '_resolveParam',
#      '_set',
#      '_setDefault',
#      '_shouldOwn',
#      '_to_java',
#      '_transfer_param_map_from_java',
#      '_transfer_param_map_to_java',
#      '_transfer_params_from_java',
#      '_transfer_params_to_java',
#      '_transform',
#      'coldStartStrategy',
#      'copy',
#      'explainParam',
#      'explainParams',
#      'extractParamMap',
#      'getOrDefault',
#      'getParam',
#      'hasDefault',
#      'hasParam',
#      'isDefined',
#      'isSet',
#      'itemCol',
#      'itemFactors',
#      'load',
#      'params',
#      'predictionCol',
#      'rank',
#      'read',
#      'recommendForAllItems',
#      'recommendForAllUsers',
#      'recommendForItemSubset',
#      'recommendForUserSubset',
#      'save',
#      'set',
#      'transform',
#      'uid',
#      'userCol',
#      'userFactors',
#      'write']




# it has some methods such as                                        

#  'recommendForAllItems' 
#  'recommendForAllUsers' 
#  'recommendForItemSubset', 
#  'recommendForUserSubset'

# so,we can use them to recomend .



















