tasteprofile_triplets.show(10)      
 
#    +--------------------+------------------+----------+
#    |                user|              song|play_count|
#    +--------------------+------------------+----------+
#    |f1bfc2a4597a3642f...|SOQEFDN12AB017C52B|         1|
#    |f1bfc2a4597a3642f...|SOQOIUJ12A6701DAA7|         2|
#    |f1bfc2a4597a3642f...|SOQOKKD12A6701F92E|         4|
#    |f1bfc2a4597a3642f...|SOSDVHO12AB01882C7|         1|
#    |f1bfc2a4597a3642f...|SOSKICX12A6701F932|         1|
#    |f1bfc2a4597a3642f...|SOSNUPV12A8C13939B|         1|
#    |f1bfc2a4597a3642f...|SOSVMII12A6701F92D|         1|
#    |f1bfc2a4597a3642f...|SOTUNHI12B0B80AFE2|         1|
#    |f1bfc2a4597a3642f...|SOTXLTZ12AB017C535|         1|
#    |f1bfc2a4597a3642f...|SOTZDDX12A6701F935|         1|
#    +--------------------+------------------+----------+
#    only showing top 10 rows


b = b.withColumn(
	"song1",
	b.song).drop("song")

b.show(5,)
#    +------------------+----------+
#    |              song1|count_play|
#    +------------------+----------+
#    |SOBONKR12A58A7A7E0 |    726885|
#    |SOAUWYT12A81C206F1 |    648239|
#    |SOSXLTC12AF72A7F54 |    527893|
#    |SOFRQTD12A81C233C0 |    425463|
#    |SOEGIYH12A6D4FC0E3 |    389880|
#    +------------------ +----------+
#    only showing top 5 rows


d = d.withColumn(
	"user1",
	d.user).drop("user")
d.show(5,)
#    +--------------------+-------------+
#    |                user1|user_activity|
#    +--------------------+-------------+
#    |ec6dfcf19485cb011... |         4400|
#    |8cb51abc6bf8ea293... |         1651|
#    |fef771ab021c20018... |         1614|
#    |5a3417a1955d91364... |         1604|
#    |c1255748c06ee3f64... |         1566|
#    +-------------------- +-------------+
#    only showing top 5 rows

-------------------------------------------------------------------------------------------

#  choose N:10000,
#  choose M:100

# it means that :
# if the count of play times is less than 10000, the song are to be recommended; and if the songs that user listen to is less than 100, they will not be recommended.

## clean:

clean_song = b.filter(b.count_play >= 10000)
clean_song.count()
1693

clean_user = d.filter(d.user_activity >= 100)
clean_user.count()
112,105
------------------------------------------------------------------------------------------



# so, here my job is :
#   Recommend the most popular songs(1693 songs) to the most active users(112,105 users)




# Get the cleaned dataset


cleaned_triplets = tasteprofile_triplets.join(clean_song,tasteprofile_triplets.song == clean_song.song1,"inner").drop("song1")
cleaned_triplets= cleaned_triplets.join(clean_user,cleaned_triplets.user == clean_user.user1,"inner").drop("user1")

cleaned_triplets.show(10,)

#    +--------------------+------------------+----------+----------+-------------+
#    |                user|              song|play_count|count_play|user_activity|
#    +--------------------+------------------+----------+----------+-------------+
#    |0000bb531aaa657c9...|SOPJLFV12A6701C797|         1|     24502|          148|
#    |0000bb531aaa657c9...|SOGDDKR12A6701E8FA|         1|     20584|          148|
#    |0000bb531aaa657c9...|SOVEUVC12A6310EAF1|         1|     23267|          148|
#    |0000bb531aaa657c9...|SOKUWEV12A8C13BBEB|         2|     13099|          148|
#    |0000bb531aaa657c9...|SOFSGLT12AB018007B|         2|     10576|          148|
#    |0000bb531aaa657c9...|SONGCCY12A67021505|         1|     11585|          148|
#    |00038cf792e9f9a1c...|SOSCIZP12AB0181D2F|         4|    111615|          105|
#    |00038cf792e9f9a1c...|SOFKFXC12AC90732A5|        11|     31672|          105|
#    |00038cf792e9f9a1c...|SOTEGWG12AB01897AC|         3|     63860|          105|
#    |00038cf792e9f9a1c...|SOGCWUH12AC90732B0|        14|     61568|          105|
#    +--------------------+------------------+----------+----------+-------------+
#    only showing top 10 rows





