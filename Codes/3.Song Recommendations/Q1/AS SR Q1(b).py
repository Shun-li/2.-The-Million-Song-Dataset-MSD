# groupby the song ,and count the play_count to get the most active users played.

a = tasteprofile_triplets.groupBy("song").agg(F.sum("play_count").alias("count_play"))
from pyspark.sql.functions import desc
b =  a.sort(desc("count_play"))
#            +------------------+----------+
#       ...: |              song|count_play|
#       ...: +------------------+----------+
#       ...: |SOBONKR12A58A7A7E0|    726885|
#       ...: |SOAUWYT12A81C206F1|    648239|
#       ...: |SOSXLTC12AF72A7F54|    527893|
#       ...: |SOFRQTD12A81C233C0|    425463|
#       ...: |SOEGIYH12A6D4FC0E3|    389880|
#       ...: |SOAXGDH12A8C13F8A1|    356533|
#       ...: |SONYKOW12AB01849C9|    292642|
#       ...: |SOPUCYA12A8C13A694|    274627|
#       ...: |SOUFTBI12AB0183F65|    268353|
#       ...: |SOVDSJC12A58A7A271|    244730|
#            +------------------+----------+
#       only showing top 10 rows


# here  identify that if the number of play_count is over 10,000, the song is a most active.  
# so ,calculate the result : 1693 songs are most active.
# the percentage of total number of unique songs:: 1693/38546 = 0.04392154827997717






  










































