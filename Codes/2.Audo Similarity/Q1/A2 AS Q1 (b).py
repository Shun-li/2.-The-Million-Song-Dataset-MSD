#glance the strcture of each dataset

hdfs dfs -text hdfs:///data/msd/genre/msd-MAGD-genreAssignment.tsv | head -10

#---------------------------------------------------------------------
#TRAAAAK128F9318786      Pop_Rock
#TRAAAAV128F421A322      Pop_Rock
#TRAAAAW128F429D538      Rap
#TRAAABD128F429CF47      Pop_Rock
#TRAAACV128F423E09E      Pop_Rock
#TRAAADT12903CCC339      Easy_Listening
#TRAAAED128E0783FAB      Vocal
#TRAAAEF128F4273421      Pop_Rock
#TRAAAEM128F93347B9      Electronic
#TRAAAFD128F92F423A      Pop_Rock
#text: Unable to write to output stream.
---------------------------------------------------------------------
hdfs dfs -text hdfs:///data/msd/genre/msd-MASD-styleAssignment.tsv | head -10
---------------------------------------------------------------------
#
#TRAAAAK128F9318786      Metal_Alternative
#TRAAAAV128F421A322      Punk
#TRAAAAW128F429D538      Hip_Hop_Rap
#TRAAACV128F423E09E      Rock_Neo_Psychedelia
#TRAAAEF128F4273421      Pop_Indie
#TRAAAFP128F931B4E3      Hip_Hop_Rap
#TRAAAGR128F425B14B      Pop_Contemporary
#TRAAAHD128F42635A5      Rock_Hard
#TRAAAHJ128F931194C      Pop_Indie
#TRAAAHZ128E0799171      Hip_Hop_Rap
#text: Unable to write to output stream.
---------------------------------------------------------------------

hdfs dfs -text hdfs:///data/msd/genre/msd-topMAGD-genreAssignment.tsv | head -10
---------------------------------------------------------------------
#TRAAAAK128F9318786      Pop_Rock
#TRAAAAV128F421A322      Pop_Rock
#TRAAAAW128F429D538      Rap
#TRAAABD128F429CF47      Pop_Rock
#TRAAACV128F423E09E      Pop_Rock
#TRAAAED128E0783FAB      Vocal
#TRAAAEF128F4273421      Pop_Rock
#TRAAAEM128F93347B9      Electronic
#TRAAAFD128F92F423A      Pop_Rock
#TRAAAFP128F931B4E3      Rap
#text: Unable to write to output stream.
---------------------------------------------------------------------


# aLL of the genre datasets  are formalled as (TrackID, GenreType)
# Load


schema_genre = StructType([
	StructField("TrackID",StringType(),True),
    StructField("GenreType",StringType(),True)
    
])


genres =  (
 	        sqlContext
 	        .read
 	        .format("com.databricks.spark.csv")
 	        .option("header","true")
 	        .option("inferschema","true")
 	        .schema(schema_genre)
            .load("/data/msd/genre/*",sep = "\t"))

genres.show(5,)
-------------------------------------------------------------------------------------
#+------------------+--------------+
#|           TrackID|     GenreType|
#+------------------+--------------+
#|TRAAAAV128F421A322|      Pop_Rock|
#|TRAAAAW128F429D538|           Rap|
#|TRAAABD128F429CF47|      Pop_Rock|
#|TRAAACV128F423E09E|      Pop_Rock|
#|TRAAADT12903CCC339|Easy_Listening|
#+------------------+--------------+
#only showing top 5 rows
-----------------------------------------------------------------------------------------



# Visualize 

genretype = genres.groupBy("GenreType").agg((F.count("TrackID").alias("count")))

genretype.show()
#---------------------------------------------------------------------------------------
#+-------------------+------+
#|          GenreType| count|
#+-------------------+------+
#|              Blues| 13672|
#|               Folk| 11730|
#|      International| 28484|
#|             Stage |  1614|
#|           Children|   477|
#|        Hip_Hop_Rap| 16100|
#|Country_Traditional| 11164|
#|             Gospel|  6974|
#| Folk_International|  9849|
#|        Electronica| 10987|
#|           Big_Band|  3115|
#|           RnB_Soul|  6238|
#|         Grunge_Emo|  6256|
#|           Pop_Rock|477570|
#|                Rap| 41878|
#|              Vocal| 12390|
#|                RnB| 28670|
#|          Religious|  8814|
#|            Country| 23544|
#|        Avant_Garde|  1014|
#+-------------------+------+
#only showing top 20 rows
----------------------------------------------------------------------------------------


# extract  data

genretype.write.format("csv").save("hdfs:///user/sli171/outputs/genretype")

hdfs dfs -ls hdfs:///user/sli171/outputs
hdfs dfs -copyToLocal hdfs:///user/sli171/outputs/genretype


#using R to dreate a bar chart









