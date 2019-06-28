## Choice of algorithms:

# choice1:
   # Logistics Regression

# choice2:
   # Navie Bay

# choice3:
   # Random Forest

---------------------------------------------------------------------------------------------------------------------------------------

# 
# Because the features exist multicollinearity,so when before using them to train each model, we need to solve this probelms.
# the methods that we can choose are :

# 1. keep important explanatory variables ,and remove secondary or alternative explanatory variables
# 2. Principle component analysis
# 3. Ridge regression
# 4. Stepwise regression
# and  so on .

# we can choose one method to avoid the multicollinearity.

----------------------------------------------------------------------------------------------------------------------------------------  

# here, I just use the most simple one : remove the not important variables:

   #  correlated strongly.
        # D3 amd A3
        # D4 and A4
        # D5 and D8,A5,A8
        # D6 and D9,A6,A9
        # D7 and A7
        # D8 and D5, A5,A8
        # D9 and D6,A6,A9
        # D10 and A10
        # A5 and A8
        # A6 and A9 

# remove the 

    #  D8 D9
    #  A3,A4,A5,A6,A7,A8,A9,A10
correlation_columns = ["D8","D9","A3","A4","A5","A6","A7","A8","A9","A10"]
for i in correlation_columns:
    genre_song  = genre_song.drop(i)

genre_song.show(5,)

# +------+------+-------+-------+-------+-------+--------+--------+-----+-------+------------------+--------------+
# |    D1|    D2|     D3|     D4|     D5|     D6|      D7|     D10|   A1|     A2|      MSD_TRACKID1|     GenreType|
# +------+------+-------+-------+-------+-------+--------+--------+-----+-------+------------------+--------------+
# | 1.087|3359.0|20300.0|4.005E7|2.396E8|1.433E9|8.817E11|1.869E14|2.482| 5768.0|TRAAABD128F429CF47|      Pop_Rock|          
# | 1.087|3359.0|20300.0|4.005E7|2.396E8|1.433E9|8.817E11|1.869E14|2.482| 5768.0|TRAAABD128F429CF47|      Pop_Rock|            
# |0.7689|6721.0|35190.0| 1.61E8|  8.5E8|4.494E9|7.113E12|1.058E15|1.525|11600.0|TRAAADT12903CCC339|Easy_Listening|           
# | 1.003|3356.0|24260.0|3.997E7| 2.82E8|1.987E9|8.791E11| 3.02E14|3.057| 5763.0|TRAAAEF128F4273421|      Pop_Rock|  
# | 1.003|3356.0|24260.0|3.997E7| 2.82E8|1.987E9|8.791E11| 3.02E14|3.057| 5763.0|TRAAAEF128F4273421|      Pop_Rock|
# +------+------+-------+-------+-------+-------+--------+--------+-----+-------+------------------+--------------+
# only showing top 5 rows

