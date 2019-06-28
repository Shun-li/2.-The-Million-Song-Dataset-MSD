# subsampling 

genre_sample = genre_song.sampleBy("is_electronic", fractions={0: 0.08, 1:1.0}, seed=0)
genre_sample.show(5,)

genre_sample.groupBy("is_electronic").count().orderBy("is_electronic").show()

#  +-------------+-----+
#  |is_electronic|count|
#  +-------------+-----+
#  |            0|82048|
#  |            1|81322|
#  +-------------+-----+

# it looks balanced.

------------------------------------------------------------------------------------------------
#Method1:

# creat vector
colname = ["D1","D2","D3","D4","D5","D6","D7","D10","A1","A2"]
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler

assembler = VectorAssembler(inputCols=colname, outputCol="features")
stages = []
stages += [assembler]
 
  

from pyspark.ml import Pipeline
pipeline = Pipeline(stages = stages)


pipelineModel = pipeline.fit(genre_sample)
genre_sample = pipelineModel.transform(genre_sample)

genre_sample.show(5,)

#    +------+------+-------+-------+-------+-------+--------+--------+-----+-------+------------------+-----------+-------------+--------------------+
#    |    D1|    D2|     D3|     D4|     D5|     D6|      D7|     D10|   A1|     A2|      MSD_TRACKID1|  GenreType|is_electronic|            features|
#    +------+------+-------+-------+-------+-------+--------+--------+-----+-------+------------------+-----------+-------------+--------------------+
#    |0.6274|6712.0|44180.0|1.606E8| 1.06E9|  7.0E9|7.085E12|2.042E15|2.939|11590.0|TRAABBY128F930C3B5|   Pop_Rock|            0|[0.6274,6712.0,44...|
#    | 2.473|3357.0|25210.0| 3.99E7|2.989E8|2.239E9|8.764E11|3.656E14|6.174| 5746.0|TRAABOG128F42955B1|   Pop_Rock|            0|[2.473,3357.0,252...|
#    |0.9117|3353.0|30930.0|3.994E7|  3.7E8|3.426E9|8.779E11|6.981E14|4.898| 5765.0|TRAABPK128F424CFDB|Metal_Death|            0|[0.9117,3353.0,30...|
#    | 2.061|6706.0|35240.0|1.604E8|8.681E8|4.693E9| 7.07E12|1.145E15|4.687|11580.0|TRAACLG128F4276511| Electronic|            1|[2.061,6706.0,352...|
#    | 2.061|6706.0|35240.0|1.604E8|8.681E8|4.693E9| 7.07E12|1.145E15|4.687|11580.0|TRAACLG128F4276511| Electronic|            1|[2.061,6706.0,352...|
#    +------+------+-------+-------+-------+-------+--------+--------+-----+-------+------------------+-----------+-------------+--------------------+
#    only showing top 5 rows




# split :

traing, test = genre_sample.randomSplit([0.6,0.4])
traing.cache()
test.cache()


print("Training Dataset Count: " + str(traing.count()))
print("Test Dataset Count: " + str(test.count()))


# Training Dataset Count: 98581
# Test Dataset Count: 64611











































--------------------------------------------------------------------------------------------------------



# Method2 : 
   #   change the numeric column to String column: 

# change the type of columns 
a = genre_sample.withColumn("D1",genre_sample.D1.cast(StringType()))
a = a.withColumn("D2",a.D2.cast(StringType()))
a = a.withColumn("D3",a.D3.cast(StringType()))
a = a.withColumn("D4",a.D4.cast(StringType()))
a = a.withColumn("D5",a.D5.cast(StringType()))
a = a.withColumn("D6",a.D6.cast(StringType()))
a = a.withColumn("D7",a.D7.cast(StringType()))
a = a.withColumn("D10",a.D10.cast(StringType()))
a = a.withColumn("A1",a.A1.cast(StringType()))
genre_sample = a.withColumn("A2",a.A2.cast(StringType()))

# check the type of genre_sample
genre_sample.dtypes

#  [('D1', 'string'),
#   ('D2', 'string'),
#   ('D3', 'string'),
#   ('D4', 'string'),
#   ('D5', 'string'),
#   ('D6', 'string'),
#   ('D7', 'string'),
#   ('D10', 'string'),
#   ('A1', 'string'),
#   ('A2', 'string'),
#   ('MSD_TRACKID1', 'string'),
#   ('GenreType', 'string'),
#   ('is_electronic', 'int')]

genre_sample = genre_sample.withColumn("features",F.concat_ws(",", 
	                                                            "D1" ,"D2","D3",
	                                                            "D4" ,"D5","D6",
	                                                         "D7","D10","A1","A2" ))

colname = ["D1","D2","D3","D4","D5","D6","D7","D10","A1","A2"]
for i in colname:
    genre_sample  = genre_sample.drop(i)
genre_sample.show(5,)

#  +------------------+-----------+-------------+--------------------+
#  |      MSD_TRACKID1|  GenreType|is_electronic|            features|        
#  +------------------+-----------+-------------+--------------------+
#  |TRAABBY128F930C3B5|   Pop_Rock|            0|0.6274,6712.0,441...|
#  |TRAABOG128F42955B1|   Pop_Rock|            0|2.473,3357.0,2521...|
#  |TRAABPK128F424CFDB|Metal_Death|            0|0.9117,3353.0,309...|
#  |TRAACLG128F4276511| Electronic|            1|2.061,6706.0,3524...|
#  |TRAACLG128F4276511| Electronic|            1|2.061,6706.0,3524...|
#  +------------------+-----------+-------------+--------------------+
#  only showing top 5 rows


#create the feature-vector
# regular expression tokenizer
from pyspark.ml.feature import RegexTokenizer
regexTokenizer = RegexTokenizer(inputCol="features", outputCol="feature", pattern=",")
genre_sample = regexTokenizer.transform(genre_sample)
genre_sample.show(5,)
#  +------------------+-----------+-------------+--------------------+--------------------+
#  |      MSD_TRACKID1|  GenreType|is_electronic|            features|             feature|
#  +------------------+-----------+-------------+--------------------+--------------------+
#  |TRAABBY128F930C3B5|   Pop_Rock|            0|0.6274,6712.0,441...|[0.6274, 6712.0, ...|
#  |TRAABOG128F42955B1|   Pop_Rock|            0|2.473,3357.0,2521...|[2.473, 3357.0, 2...|
#  |TRAABPK128F424CFDB|Metal_Death|            0|0.9117,3353.0,309...|[0.9117, 3353.0, ...|
#  |TRAACLG128F4276511| Electronic|            1|2.061,6706.0,3524...|[2.061, 6706.0, 3...|
#  |TRAACLG128F4276511| Electronic|            1|2.061,6706.0,3524...|[2.061, 6706.0, 3...|
#  +------------------+-----------+-------------+--------------------+--------------------+
#  only showing top 5 rows




# BOW
from pyspark.ml.feature import CountVectorizer
vectorizer = CountVectorizer(inputCol="feature", outputCol="bow", vocabSize=1000)
bow_model = vectorizer.fit(genre_sample)
genre_sample = bow_model.transform(genre_sample)

genre_sample.show(5,)

#  +------------------+--------------------+-------------+--------------------+--------------------+--------------------+
#  |      MSD_TRACKID1|           GenreType|is_electronic|            features|             feature|                 bow|
#  +------------------+--------------------+-------------+--------------------+--------------------+--------------------+
#  |TRAABBY128F930C3B5|Rock_Neo_Psychedelia|            0|0.6274,6712.0,441...|[0.6274, 6712.0, ...|(1000,[1,4,14,70,...|
#  |TRAABOG128F42955B1|   Rock_Contemporary|            0|2.473,3357.0,2521...|[2.473, 3357.0, 2...|(1000,[23,114,294...|
#  |TRAABPK128F424CFDB|            Pop_Rock|            0|0.9117,3353.0,309...|[0.9117, 3353.0, ...|(1000,[25,43,75,1...|
#  |TRAACLG128F4276511|          Electronic|            1|2.061,6706.0,3524...|[2.061, 6706.0, 3...|(1000,[0,24,247,3...|
#  |TRAACLG128F4276511|               Dance|            0|2.061,6706.0,3524...|[2.061, 6706.0, 3...|(1000,[0,24,247,3...|
#  +------------------+--------------------+-------------+--------------------+--------------------+--------------------+
#  only showing top 5 rows




# TF-IDF

from pyspark.ml.feature import IDF
idf = IDF(inputCol="bow", outputCol="tfidf")
idf_model = idf.fit(genre_sample)
genre_sample = idf_model.transform(genre_sample)

genre_sample.show(5,)

#   +------------------+--------------------+-------------+--------------------+--------------------+--------------------+--------------------+
#   |      MSD_TRACKID1|           GenreType|is_electronic|            features|             feature|                 bow|               tfidf|
#   +------------------+--------------------+-------------+--------------------+--------------------+--------------------+--------------------+
#   |TRAABBY128F930C3B5|Rock_Neo_Psychedelia|            0|0.6274,6712.0,441...|[0.6274, 6712.0, ...|(1000,[1,4,14,70,...|(1000,[1,4,14,70,...|
#   |TRAABOG128F42955B1|   Rock_Contemporary|            0|2.473,3357.0,2521...|[2.473, 3357.0, 2...|(1000,[23,114,294...|(1000,[23,114,294...|
#   |TRAABPK128F424CFDB|            Pop_Rock|            0|0.9117,3353.0,309...|[0.9117, 3353.0, ...|(1000,[25,43,75,1...|(1000,[25,43,75,1...|
#   |TRAACLG128F4276511|          Electronic|            1|2.061,6706.0,3524...|[2.061, 6706.0, 3...|(1000,[0,24,247,3...|(1000,[0,24,247,3...|
#   |TRAACLG128F4276511|               Dance|            0|2.061,6706.0,3524...|[2.061, 6706.0, 3...|(1000,[0,24,247,3...|(1000,[0,24,247,3...|
#   +------------------+--------------------+-------------+--------------------+--------------------+--------------------+--------------------+
#   only showing top 5 rows


# create the label column (StringIndex)

from pyspark.ml.feature import  StringIndexer
label_stringIdx = StringIndexer(inputCol = "is_electronic", outputCol = "label")
pipelineFit = label_stringIdx.fit(genre_sample)
genre_sample = pipelineFit.transform(genre_sample)
genre_sample.show(5)

#    +------------------+-----------+-------------+--------------------+--------------------+--------------------+--------------------+-----+
#    |      MSD_TRACKID1|  GenreType|is_electronic|            features|             feature|                 bow|               tfidf|label|
#    +------------------+-----------+-------------+--------------------+--------------------+--------------------+--------------------+-----+
#    |TRAABBY128F930C3B5|   Pop_Rock|            0|0.6274,6712.0,441...|[0.6274, 6712.0, ...|(1000,[1,4,14,70,...|(1000,[1,4,14,70,...|  0.0|
#    |TRAABOG128F42955B1|   Pop_Rock|            0|2.473,3357.0,2521...|[2.473, 3357.0, 2...|(1000,[23,114,294...|(1000,[23,114,294...|  0.0|
#    |TRAABPK128F424CFDB|Metal_Death|            0|0.9117,3353.0,309...|[0.9117, 3353.0, ...|(1000,[25,43,75,1...|(1000,[25,43,75,1...|  0.0|
#    |TRAACLG128F4276511| Electronic|            1|2.061,6706.0,3524...|[2.061, 6706.0, 3...|(1000,[0,24,247,3...|(1000,[0,24,247,3...|  1.0|
#    |TRAACLG128F4276511| Electronic|            1|2.061,6706.0,3524...|[2.061, 6706.0, 3...|(1000,[0,24,247,3...|(1000,[0,24,247,3...|  1.0|
#    +------------------+-----------+-------------+--------------------+--------------------+--------------------+--------------------+-----+
#    only showing top 5 rows








