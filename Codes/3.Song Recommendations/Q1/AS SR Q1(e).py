

# first , I need to convert the user column into an innteger index that represents the user, and convert the song column into 

# integer index that represesnt the song:


from pyspark.ml.feature import  StringIndexer

label_stringIdx = StringIndexer(inputCol = "user", outputCol = "userID")
pipelineFit = label_stringIdx.fit(cleaned_triplets)
cleaned_triplets = pipelineFit.transform(cleaned_triplets)

label_stringIdx = StringIndexer(inputCol = "song", outputCol = "songID")
pipelineFit = label_stringIdx.fit(cleaned_triplets)
cleaned_triplets = pipelineFit.transform(cleaned_triplets)

cleaned_triplets = cleaned_triplets.withColumn("userID1", F.round(cleaned_triplets.userID,6).cast("integer")).drop("userID")
cleaned_triplets = cleaned_triplets.withColumn("songID1", F.round(cleaned_triplets.songID,6).cast("integer")).drop("songID")


cleaned_triplets.show(20,)

#      +--------------------+------------------+----------+----------+-------------+-------+-------+
#      |                user|              song|play_count|count_play|user_activity|userID1|songID1|
#      +--------------------+------------------+----------+----------+-------------+-------+-------+
#      |0000bb531aaa657c9...|SOFSGLT12AB018007B|         2|     10576|          148| 106374|   1276|
#      |0000bb531aaa657c9...|SOKUWEV12A8C13BBEB|         2|     13099|          148| 106374|   1146|
#      |0000bb531aaa657c9...|SOVEUVC12A6310EAF1|         1|     23267|          148| 106374|    814|
#      |00038cf792e9f9a1c...|SOANQFY12AB0183239|         2|     87050|          105|   9901|     41|
#      |00038cf792e9f9a1c...|SOAXGDH12A8C13F8A1|        11|    356533|          105|   9901|      0|
#      |00038cf792e9f9a1c...|SOBADEB12AB018275F|         8|     62438|          105|   9901|     81|
#      |00038cf792e9f9a1c...|SOBOAFP12A8C131F36|         3|    127044|          105|   9901|     22|
#      |00038cf792e9f9a1c...|SOCKSGZ12A58A7CA4B|         6|     96692|          105|   9901|     32|
#      |00038cf792e9f9a1c...|SOCVTLJ12A6310F0FD|         3|    114362|          105|   9901|      7|
#      |00038cf792e9f9a1c...|SODCLQR12A67AE110D|         1|     34382|          105|   9901|    273|
#      |00038cf792e9f9a1c...|SODGJKH12AAA8C9487|         1|     79974|          105|   9901|     64|
#      |00038cf792e9f9a1c...|SODGVGW12AC9075A8D|        18|    110757|          105|   9901|     53|
#      |00038cf792e9f9a1c...|SOEYVHS12AB0181D31|         1|     37957|          105|   9901|    135|
#      |00038cf792e9f9a1c...|SOFKABN12A8AE476C6|        13|    105032|          105|   9901|     18|
#      |00038cf792e9f9a1c...|SOFKFXC12AC90732A5|        11|     31672|          105|   9901|    408|
#      |00038cf792e9f9a1c...|SOFRQTD12A81C233C0|        15|    425463|          105|   9901|      1|
#      |00038cf792e9f9a1c...|SOGDDKR12A6701E8FA|         2|     20584|          105|   9901|    564|
#      |00038cf792e9f9a1c...|SOGPBAW12A6D4F9F22|         1|     83616|          105|   9901|     36|
#      |00038cf792e9f9a1c...|SOGSAYQ12AB018BA14|        17|     66998|          105|   9901|    131|
#      |00038cf792e9f9a1c...|SOHEMBB12A6701E907|         1|     45328|          105|   9901|    140|
#      +--------------------+------------------+----------+----------+-------------+-------+-------+



------------------------------------------------------------------------------------------------------

# Make sure that every user in the test has some user-songs-plays in the  traininhg set as well.


#so,here my thinking is that :




#step1:
#   Randomsplit data into 70% for training , and 30% for testing;
#   Then use the dropDuplicates command to get the unique user in training and testing data.

training_data, testing_data = cleaned_triplets.randomSplit([0.7,0.3])
n = training_data.dropDuplicates(["user"])
m=  testing_data.dropDuplicates(["user"])

#step2:
#get the co-owned users from the two unique user datasets.

shared_data = m.join(n,"user","inner")
shared_data = shared_data.select("user")
 




#step3:
#   filter the beginning  training and testing data to make them just cover these co-owned users.

training_data = training_data.join(shared_data,"user","inner")
testing_data = testing_data.join(shared_data,"user","inner")

training_data.show(5,)
testing_data.show(5,)

print(cleaned_triplets.count())
#  3905520
print(training_data.count())
#  2721715
print(testing_data.count())
#  1172052
print(m.count())
#  109380
print(n.count())
# 111563
print(shared_data.count())
# 109104


# step4:
# check whether the every user that in the test set is also covered in the training set

a = training_data.dropDuplicates(["user"])
b =  testing_data.dropDuplicates(["user"])
print(a.count())
# 109104

print(b.count())
# 109104


# a = b = shared_data , so now , we can determine that every user in the test set has user-song-plays in training set as well.


#step5:
print("Training Dataset Count: " + str(training_data.count()))
print("Test Dataset Count: " + str(testing_data.count()))

#  Training Dataset Count: 2721715 (69.90%)
#  Test Dataset Count: 1172052 (30.10%)
 

-----------------------------------------------------------------------------------------------------------------------------------------------------