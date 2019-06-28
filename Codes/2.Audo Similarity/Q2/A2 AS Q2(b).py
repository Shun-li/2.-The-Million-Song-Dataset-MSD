# convert

# 1: stands for "Electronic"
# 0: stands for others

genre_song = genre_song.withColumn(
	"is_electronic",
	 F.when(genre_song.GenreType == "Electronic",1)
	  .otherwise(0)
	  )

genre_song.show(5,)


#  +------+------+-------+-------+-------+-------+--------+--------+-----+-------+------------------+--------------+-------------+
#  |    D1|    D2|     D3|     D4|     D5|     D6|      D7|     D10|   A1|     A2|      MSD_TRACKID1|     GenreType|is_electronic|
#  +------+------+-------+-------+-------+-------+--------+--------+-----+-------+------------------+--------------+-------------+
#  | 1.087|3359.0|20300.0|4.005E7|2.396E8|1.433E9|8.817E11|1.869E14|2.482| 5768.0|TRAAABD128F429CF47|      Pop_Rock|            0|
#  | 1.087|3359.0|20300.0|4.005E7|2.396E8|1.433E9|8.817E11|1.869E14|2.482| 5768.0|TRAAABD128F429CF47|      Pop_Rock|            0|
#  |0.7689|6721.0|35190.0| 1.61E8|  8.5E8|4.494E9|7.113E12|1.058E15|1.525|11600.0|TRAAADT12903CCC339|Easy_Listening|            0|
#  | 1.003|3356.0|24260.0|3.997E7| 2.82E8|1.987E9|8.791E11| 3.02E14|3.057| 5763.0|TRAAAEF128F4273421|      Pop_Rock|            0|
#  | 1.003|3356.0|24260.0|3.997E7| 2.82E8|1.987E9|8.791E11| 3.02E14|3.057| 5763.0|TRAAAEF128F4273421|      Pop_Rock|            0|
#  +------+------+-------+-------+-------+-------+--------+--------+-----+-------+------------------+--------------+-------------+
#  only showing top 5 rows




# Class balance of binary label:
#  for this question, let's check how many percentage of observations are "Electronic" 
#   and how many percentage of observations are others.
#   if the percentage of "Electronic" is much less than others, it means 
#   the data are is unbalance

genre_song.filter(genre_song.is_electronic== 1).count()
81322
genre_song.filter(genre_song.is_electronic== 0).count()
1016259

#   ------------------
#   |    0    |   1  |
#   |---------|------|
#   | 1016259 | 81322|
#   ------------------
#   |  92.6%  | 7.4% |
#   ------------------


# so , for this case , this is a severely unbalanced dataset.

# we need to balance it, by the following methods:

   # up sample/oversampling :        produce more instances of the under represented class. 
   # down sample/ undersampling  :   discard instances of over represented class
   # both/ stratification  :         do a metric of up and down samplimng


   




   
