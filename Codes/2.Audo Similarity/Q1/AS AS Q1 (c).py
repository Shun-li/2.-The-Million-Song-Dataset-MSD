

#features:


----------------------------------------------------------------------------------------

# +------+------+-------+-------+-------+--------+--------+--------+--------+--------+-----+-------+-------+--------+--------+---------+--------+--------+--------+--------+--------------------+
# |    D1|    D2|     D3|     D4|     D5|      D6|      D7|      D8|      D9|     D10|   A1|     A2|     A3|      A4|      A5|       A6|      A7|      A8|      A9|     A10|         MSD_TRACKID|
# +------+------+-------+-------+-------+--------+--------+--------+--------+--------+-----+-------+-------+--------+--------+---------+--------+--------+--------+--------+--------------------+
# |0.9295|6720.0|44100.0|1.608E8| 1.06E9| 6.985E9|7.095E12| 9.545E9|6.293E10|2.037E15|2.666|11580.0|74040.0|-1.792E8|-1.153E9|  -7.42E9|6.242E12|1.037E10| 6.68E10|1.694E15|'TRHFHYX12903CAF953'|
# | 1.883|6712.0|49060.0|1.606E8|1.176E9| 8.609E9|7.083E12|1.058E10|7.744E10|2.781E15| 8.87|11580.0|85200.0|-1.791E8|-1.316E9|  -9.66E9|6.233E12|1.182E10| 8.68E10|2.463E15|'TRHFHAU128F9341A0E'|
# | 1.884|6722.0|56130.0| 1.61E8|1.346E9|1.127E10|7.112E12|1.211E10|1.014E11|4.193E15|4.328|11600.0|93320.0|-1.797E8|-1.459E9|-1.185E10|6.262E12|1.311E10|1.066E11|3.432E15|'TRHFHLP128F14947A7'|
# |  1.52|6709.0|53230.0|1.605E8|1.295E9|1.045E10|7.076E12|1.164E10|9.392E10|3.751E15|8.452|11580.0|93650.0| -1.79E8|-1.441E9|-1.159E10| 6.23E12|1.293E10|1.041E11|3.248E15|'TRHFHFF128F930AC11'|
# | 1.363|6710.0|28750.0|1.605E8|  6.9E8| 2.965E9|7.075E12| 6.194E9|2.663E10|5.621E14|2.787|11580.0|49990.0| -1.79E8|-7.713E8| -3.322E9|6.227E12|  6.92E9|2.983E10| 4.97E14|'TRHFHYJ128F4234782'|
# +------+------+-------+-------+-------+--------+--------+--------+--------+--------+-----+-------+-------+--------+--------+---------+--------+--------+--------+--------+--------------------+
# only showing top 5 rows

----------------------------------------------------------------------------------------

#genres:


----------------------------------------------------------------------------------------
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


# Merge

genre_song  = features.join(genres,features.MSD_TRACKID1 == genres.TrackID).drop("TrackID")

genre_song.show(5,)

#+------+------+-------+-------+-------+-------+--------+-------+--------+--------+-----+-------+-------+--------+--------+--------+--------+-------+--------+--------+------------------+--------------+
#|    D1|    D2|     D3|     D4|     D5|     D6|      D7|     D8|      D9|     D10|   A1|     A2|     A3|      A4|      A5|      A6|      A7|     A8|      A9|     A10|      MSD_TRACKID1|     GenreType|
#+------+------+-------+-------+-------+-------+--------+-------+--------+--------+-----+-------+-------+--------+--------+--------+--------+-------+--------+--------+------------------+--------------+
#| 1.087|3359.0|20300.0|4.005E7|2.396E8|1.433E9|8.817E11|2.141E9|1.281E10|1.869E14|2.482| 5768.0|33690.0|-4.452E7|-2.623E8|-1.545E9|7.736E11|2.335E9|1.377E10|1.603E14|TRAAABD128F429CF47|      Pop_Rock|
#| 1.087|3359.0|20300.0|4.005E7|2.396E8|1.433E9|8.817E11|2.141E9|1.281E10|1.869E14|2.482| 5768.0|33690.0|-4.452E7|-2.623E8|-1.545E9|7.736E11|2.335E9|1.377E10|1.603E14|TRAAABD128F429CF47|      Pop_Rock|
#|0.7689|6721.0|35190.0| 1.61E8|  8.5E8|4.494E9|7.113E12|7.631E9|4.036E10|1.058E15|1.525|11600.0|57960.0|-1.797E8|-9.078E8|-4.593E9|6.265E12|8.144E9|4.123E10|8.378E14|TRAAADT12903CCC339|Easy_Listening|
#| 1.003|3356.0|24260.0|3.997E7| 2.82E8|1.987E9|8.791E11|2.528E9|1.782E10| 3.02E14|3.057| 5763.0|39420.0|-4.444E7|-3.083E8| -2.14E9|7.714E11|2.759E9|1.916E10| 2.62E14|TRAAAEF128F4273421|      Pop_Rock|
#| 1.003|3356.0|24260.0|3.997E7| 2.82E8|1.987E9|8.791E11|2.528E9|1.782E10| 3.02E14|3.057| 5763.0|39420.0|-4.444E7|-3.083E8| -2.14E9|7.714E11|2.759E9|1.916E10| 2.62E14|TRAAAEF128F4273421|     Pop_Indie|
#+------+------+-------+-------+-------+-------+--------+-------+--------+--------+-----+-------+-------+--------+--------+--------+--------+-------+--------+--------+------------------+--------------+
#only showing top 5 rows
