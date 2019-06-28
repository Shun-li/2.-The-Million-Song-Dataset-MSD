
#First , let's have a view about the structure of Taste Profile

# drwxr-xr-x    /data/msd/tasteprofile

   #1 drwxr-xr-x   /data/msd/tasteprofile/mismatches
        #-rw-r--r--    /data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt
        #-rw-r--r--    /data/msd/tasteprofile/mismatches/sid_mismatches.txt

   #2 drwxr-xr-x    /data/msd/tasteprofile/triplets.tsv
        #-rw-r--r--   /data/msd/tasteprofile/triplets.tsv/part-00000.tsv.gz
        #-rw-r--r--   /data/msd/tasteprofile/triplets.tsv/part-00001.tsv.gz
        #-rw-r--r--   /data/msd/tasteprofile/triplets.tsv/part-00002.tsv.gz
        #-rw-r--r--   /data/msd/tasteprofile/triplets.tsv/part-00003.tsv.gz
        #-rw-r--r--   /data/msd/tasteprofile/triplets.tsv/part-00004.tsv.gz
        #-rw-r--r--   /data/msd/tasteprofile/triplets.tsv/part-00005.tsv.gz
        #-rw-r--r--   /data/msd/tasteprofile/triplets.tsv/part-00006.tsv.gz
        #-rw-r--r--   /data/msd/tasteprofile/triplets.tsv/part-00007.tsv.gz

# From the blog-post website  ,we can know,in tasteprofile:
   # 1,019,318 unique users
   # 384,546 unique MSD songs
   # 48,373,586 user - song - play count triplets

   hdfs dfs -text hdfs:///data/msd/tasteprofile/triplets.tsv/* | wc -l
   48373586
-----------------------------------------------------------------------------------------------------
# for triplets:
   #  user, song, play count 
   #and each line looks like this (tab-delimited):

#eg:

 hdfs dfs -text hdfs:///data/msd/tasteprofile/triplets.tsv/part-00000.tsv.gz| head -10

#b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOAKIMP12A8C130995      1
#b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOAPDEY12A81C210A9      1
#b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBBMDR12A8C13253B      2
#b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBFNSP12AF72A0E22      1
#b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBFOVM12A58A7D494      1
#b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBNZDC12A6D4FC103      1
#b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBSUJE12A6D4F8CF5      2
#b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBVFZR12A6D4F8AE3      1
#b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBXALG12A8C13C108      1
#b80344d063b5ccb3212f76538f3d9e43d87dca9e        SOBXHDL12A81C204C0      1

-----------------------------------------------------------------------------------------------------
# for mismatches:
   #  SongID,TrackID
#eg

hdfs dfs -text hdfs:///data/msd/tasteprofile/mismatches/sid_mismatches.txt | head -10

#ERROR: <SOUMNSI12AB0182807 TRMMGKQ128F9325E10> Digital Underground  -  The Way We Swing  !=  Linkwood  -  Whats up with the Underground
#ERROR: <SOCMRBE12AB018C546 TRMMREB12903CEB1B1> Jimmy Reed  -  The Sun Is Shining (Digitally Remastered)  !=  Slim Harpo  -  I Got Love If You Want It
#ERROR: <SOLPHZY12AC468ABA8 TRMMBOC12903CEB46E> Africa HiTech  -  Footstep  !=  Marcus Worgull  -  Drumstern (BONUS TRACK)
#ERROR: <SONGHTM12A8C1374EF TRMMITP128F425D8D0> Death in Vegas  -  Anita Berber  !=  Valen Hsu  -  Shi Yi
#ERROR: <SONGXCA12A8C13E82E TRMMAYZ128F429ECE6> Grupo Exterminador  -  El Triunfador  !=  I Ribelli  -  Lei M
#ERROR: <SOMBCRC12A67ADA435 TRMMNVU128EF343EED> Fading Friend  -  Get us out!  !=  Masterboy  -  Feel The Heat 2000
#ERROR: <SOTDWDK12A8C13617B TRMMNCZ128F426FF0E> Daevid Allen  -  Past Lives  !=  Bhimsen Joshi  -  Raga - Shuddha Sarang_ Aalap
#ERROR: <SOEBURP12AB018C2FB TRMMPBS12903CE90E1> Cristian Paduraru  -  Born Again  !=  Yespiring  -  Journey Stages
#ERROR: <SOSRJHS12A6D4FDAA3 TRMWMEL128F421DA68> Jeff Mills  -  Basic Human Design  !=  M&T  -  Drumsettester
#ERROR: <SOIYAAQ12A6D4F954A TRMWHRI128F147EA8E> Excepter  -  OG  !=  The Fevers  -  Não Tenho Nada (Natchs Scheint Die Sonne)

---------------------------------------------------------------------------------------------------------------------------------------

# our job :
   # step1:

      # Load files and Extract the TrackID character and  SongID from each row of the mismatchges textfile, as mismatch file. 

   # step2:
      # Join the triplets file with mismatch file together with common "song" column.

----------------------------------------------------------------------------------------------------------------------------------------
# Step1:

# Load 

# triplets
schema_tasteprofile_triplets = StructType([
	StructField("user",StringType(),True),
	StructField("song",StringType(),True),
	StructField("play_count",IntegerType(),True)
	])

 tasteprofile_triplets =  (
 	                      sqlContext
 	                      .read
 	                      .format("com.databricks.spark.csv")
 	                      .option("header","false")
 	                      .schema(schema_tasteprofile_triplets)
                          .load("/data/msd/tasteprofile/triplets.tsv/*",sep = "\t"))

 tasteprofile_triplets.show(10)      
 
#+--------------------+------------------+----------+
#|                user|              song|play_count|
#+--------------------+------------------+----------+
#|f1bfc2a4597a3642f...|SOQEFDN12AB017C52B|         1|
#|f1bfc2a4597a3642f...|SOQOIUJ12A6701DAA7|         2|
#|f1bfc2a4597a3642f...|SOQOKKD12A6701F92E|         4|
#|f1bfc2a4597a3642f...|SOSDVHO12AB01882C7|         1|
#|f1bfc2a4597a3642f...|SOSKICX12A6701F932|         1|
#|f1bfc2a4597a3642f...|SOSNUPV12A8C13939B|         1|
#|f1bfc2a4597a3642f...|SOSVMII12A6701F92D|         1|
#|f1bfc2a4597a3642f...|SOTUNHI12B0B80AFE2|         1|
#|f1bfc2a4597a3642f...|SOTXLTZ12AB017C535|         1|
#|f1bfc2a4597a3642f...|SOTZDDX12A6701F935|         1|
#+--------------------+------------------+----------+
#only showing top 10 rows


# mismatches
tasteprofile_mismatches = (spark.read.text("/data/msd/tasteprofile/mismatches/sid_mismatches.txt"))
mismatches = tasteprofile_mismatches.select(
	         tasteprofile_mismatches.value.substr(9,18).alias("song1"),
	         tasteprofile_mismatches.value.substr(28,18).alias("track"),
	         )

mismatches.show(10) 

#-----------------+------------------+
#|             song1|             track|
#+------------------+------------------+
#|SOUMNSI12AB0182807|TRMMGKQ128F9325E10|
#|SOCMRBE12AB018C546|TRMMREB12903CEB1B1|
#|SOLPHZY12AC468ABA8|TRMMBOC12903CEB46E|
#|SONGHTM12A8C1374EF|TRMMITP128F425D8D0|
#|SONGXCA12A8C13E82E|TRMMAYZ128F429ECE6|
#|SOMBCRC12A67ADA435|TRMMNVU128EF343EED|
#|SOTDWDK12A8C13617B|TRMMNCZ128F426FF0E|
#|SOEBURP12AB018C2FB|TRMMPBS12903CE90E1|
#|SOSRJHS12A6D4FDAA3|TRMWMEL128F421DA68|
#|SOIYAAQ12A6D4F954A|TRMWHRI128F147EA8E|
#+------------------+------------------+
#only showing top 10 rows

#non- mismatch
#   +------------------+------------------+
#   |             song1|             track|
#   +------------------+------------------+
#   |                  |                  |
#   |SOFQHZM12A8C142342|TRMWMFG128F92FFEF2|
#   |                  |                  |
#   |SODXUTF12AB018A3DA|TRMWPCD12903CCE5ED|
#   |                  |                  |
#   |SOASCRF12A8C1372E6|TRMHIPJ128F426A2E2|
#   |                  |                  |
#   |SOITDUN12A58A7AACA|TRMHXGK128F42446AB|
#   |                  |                  |
#   |SOLZXUM12AB018BE39|TRMRSOF12903CCF516|
#   +------------------+------------------+
#   only showing top 10 rows



# Step2:

triplets_without_mismatches = tasteprofile_triplets.join(mismatches,tasteprofile_triplets.song == mismatches.song1,"left_anti")

triplets_without_mismatches.show(5,)

#+--------------------+------------------+----------+
#|                user|              song|play_count|
#+--------------------+------------------+----------+
#|ce520154cb63affa8...|SOEBMRN12B35058985|         1|
#|ce520154cb63affa8...|SOMKCLA12A8AE4562E|         1|
#|ce520154cb63affa8...|SOSVPIE12A6D4FA873|         2|
#|042772a58ced99061...|SOBIYBZ12AB018A40C|         1|
#|042772a58ced99061...|SOEGXYE12AF729D9FF|         1|
#+--------------------+------------------+----------+
#only showing top 5 rows

print(tasteprofile_triplets.count())
# 48373586

print(mismatches.count())
# 20032

print(triplets_without_mismatches.count())
# 45795100


  







