# Count the number of rows

 ./start-pyspark.sh
# Import

from pyspark.sql.types import *
import pyspark.sql.functions as F  
-------------------------------------------------------------------------------------------------------
#1. audio

  #1.1 audio/attributes
 
   audio_attributes = (spark.read.csv("/data/msd/audio/attributes/*"))
   audio_attributes.count()
   3929


  #1.2 audio/features

   audio_features = (spark.read.csv("/data/msd/audio/features/*"))
   audio_features.count()
   12927867

  #1.3 audio/features

   audio_statistics = (spark.read.csv("/data/msd/audio/statistics/*"))
   audio_statistics.count()
   992866

-------------------------------------------------------------------------------------------------------
#2.  genre

   genre = (spark.read.csv("/data/msd/genre/*",sep = "\t"))
   genre.count()
   1103077

-------------------------------------------------------------------------------------------------------
#3.  main
 
 #3.1 main/summary

  main_summary = (spark.read.csv("/data/msd/main/summary/*"))
  main_summary.count()
  2000002

-------------------------------------------------------------------------------------------------------
#4.  tasteprofile
 
  #4.1 tasteprofile/mismatches
 
  tasteprofile_mismatches = (spark.read.text("/data/msd/tasteprofile/mismatches/*"))
  tasteprofile_mismatches.count()
  20032

  #4.2 tasteprofile/triplets

  tasteprofile_triplets = (spark.read.csv("/data/msd/tasteprofile/triplets.tsv/*",sep = "\t"))
  tasteprofile_triplets.count() 
  48373586

-------------------------------------------------------------------------------------------------------
