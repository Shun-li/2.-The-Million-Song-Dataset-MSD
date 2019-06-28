# prediction

predictions = model.transform(testing_data)
predictions.show(10,)



#   +--------------------+------------------+----------+----------+-------------+-------+-------+----------+
#   |                user|              song|play_count|count_play|user_activity|userID1|songID1|prediction|
#   +--------------------+------------------+----------+----------+-------------+-------+-------+----------+
#   |de9f0b78bc5169a45...|SOPQLBY12A6310E992|         4|     98854|          331|     56|     12| 2.6504946|
#   |90f40228b33cd9b6e...|SOPQLBY12A6310E992|         3|     98854|          268|     67|     12| 3.8793418|
#   |a0d26046c585210e0...|SOPQLBY12A6310E992|         2|     98854|          525|     95|     12| 1.6551075|
#   |8d0ccf05c0fa9892c...|SOPQLBY12A6310E992|         4|     98854|          233|     97|     12|  4.568097|
#   |d1ca8b3e78811238c...|SOPQLBY12A6310E992|         2|     98854|          248|    107|     12| 3.3703797|
#   |258341421c013c9f8...|SOPQLBY12A6310E992|         1|     98854|          403|    115|     12|  2.450111|
#   |fe685c55b08f50400...|SOPQLBY12A6310E992|         2|     98854|          654|    161|     12| 2.0427737|
#   |b604b9641b544813d...|SOPQLBY12A6310E992|         3|     98854|          275|    177|     12| 1.8171208|
#   |fd1ebc6caa7ad07c8...|SOPQLBY12A6310E992|         2|     98854|          638|    202|     12| 2.6618793|
#   |71539efb2660534ca...|SOPQLBY12A6310E992|         1|     98854|          209|    229|     12|   5.12494|
#   |e5a682cc241293095...|SOPQLBY12A6310E992|         1|     98854|          241|    244|     12|   2.03166|
#   |fde3f992d2cccffd0...|SOPQLBY12A6310E992|         3|     98854|          239|    257|     12|  3.043432|
#   |1730cee09c90bd1ba...|SOPQLBY12A6310E992|         1|     98854|          344|    265|     12|  2.112639|
#   |f24a5cce697b295c8...|SOPQLBY12A6310E992|         4|     98854|          439|    273|     12| 2.5549579|
#   |543b9d3242599e1a7...|SOPQLBY12A6310E992|         3|     98854|          306|    280|     12| 2.3381996|
#   |3c1775e0a85323d18...|SOPQLBY12A6310E992|         1|     98854|          237|    304|     12|  1.436506|
#   |f10a7de5ec0aee34f...|SOPQLBY12A6310E992|         8|     98854|          221|    306|     12| 3.7915974|
#   |c26b880a4e765d805...|SOPQLBY12A6310E992|         3|     98854|          235|    321|     12| 2.1941159|
#   |83b29bd85d2000315...|SOPQLBY12A6310E992|         3|     98854|          688|    339|     12|  2.080185|
#   |4be305e02f4e72dad...|SOPQLBY12A6310E992|         2|     98854|          306|    363|     12| 28.102573|
#   ---------------------+------------------+----------+----------+-------------+-------+-------+----------+ 
#  only showing top 10 rows

------------------------------------------------------------------------------------------------------------

#  now, I choose the userID is 100.
#get the top 20 songs that recommendated for this user from the model generated.

user1 = testing_data.filter(testing_data.userID1 == 1)
user1.sort(desc("play_count")).show(5,)
#       +--------------------+------------------+----------+----------+-------------+-------+-------+
#       |                user|              song|play_count|count_play|user_activity|userID1|songID1|
#       +--------------------+------------------+----------+----------+-------------+-------+-------+
#       |b7c24f770be6b8028...|SOUFTBI12AB0183F65|        35|    268353|         1446|      1|     66|
#       |b7c24f770be6b8028...|SONQCXC12A6D4F6A37|        34|    115134|         1446|      1|    119|
#       |b7c24f770be6b8028...|SOBONKR12A58A7A7E0|        34|    726885|         1446|      1|     10|
#       |b7c24f770be6b8028...|SOMGIYR12AB0187973|        25|     89974|         1446|      1|     87|
#       |b7c24f770be6b8028...|SOPPROJ12AB0184E18|        24|    124162|         1446|      1|     21|
#       +--------------------+------------------+----------+----------+-------------+-------+-------+
#       only showing top 5 rows



users_recomnend = model.recommendForAllUsers(5)
user1 = users_recomnend.filter(users_recomnend.userID1 ==1)
user1.collect()

[Row(userID1=1, 
	recommendations=
	[
	   Row(songID1=1632  , rating=28.20026397705078 ),
	   Row(songID1=1363,  rating=26.385095596313477),
	   Row(songID1=1057  , rating=22.891868591308594),
	   Row(songID1=5,    rating=21.934368133544922), 
	   Row(songID1=1681, rating=21.701679229736328)
	   ]
	)
]






# compare:

# when we  check the songs'type, magority of the songs that recommendated are the same kind of songs that the user has actually played.
    















