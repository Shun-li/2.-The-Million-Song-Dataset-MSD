# Visualization:

# song popularity:

a = tasteprofile_triplets.groupBy("song").agg(F.sum("play_count").alias("count_play"))
from pyspark.sql.functions import desc
b =  a.sort(desc("count_play"))


 # extract  data

b.write.format("csv").save("hdfs:///user/sli171/outputs/song-popularity")

hdfs dfs -ls hdfs:///user/sli171/outputs
hdfs dfs -copyToLocal hdfs:///user/sli171/outputs/song-popularity

#using R to dreate a bar chart




--------------------------------------------------------------------------------------

# user activity

c = tasteprofile_triplets.groupBy("user").agg(F.count("user").alias("user_activity"))
from pyspark.sql.functions import desc
d =  c.sort(desc("user_activity"))


 # extract  data

d.write.format("csv").save("hdfs:///user/sli171/outputs/user_activity")

hdfs dfs -ls hdfs:///user/sli171/outputs
hdfs dfs -copyToLocal hdfs:///user/sli171/outputs/user_activity



#using R to dreate a bar chart


