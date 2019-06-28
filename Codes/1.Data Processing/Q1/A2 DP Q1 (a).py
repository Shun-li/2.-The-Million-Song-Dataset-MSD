assignment 2

#####1. FILE FORMATS 

# Q1
#  (a)
#   Review all of the files
hdfs dfs -ls  /data/msd/

#und 4 items
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/audio
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/genre
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/main
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/tasteprofile




hdfs dfs -ls -R /data/msd/
-------------------------------------------------------------------------------------------------------
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/audio
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:45 /data/msd/audio/attributes
#-rw-r--r--   8 hadoop supergroup       1051 2018-09-28 12:45 /data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup        671 2018-09-28 12:45 /data/msd/audio/attributes/msd-jmir-lpc-all-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup        484 2018-09-28 12:45 /data/msd/audio/attributes/msd-jmir-methods-of-moments-all-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup        898 2018-09-28 12:45 /data/msd/audio/attributes/msd-jmir-mfcc-all-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup        777 2018-09-28 12:45 /data/msd/audio/attributes/msd-jmir-spectral-all-all-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup        777 2018-09-28 12:45 /data/msd/audio/attributes/msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup      12317 2018-09-28 12:45 /data/msd/audio/attributes/msd-marsyas-timbral-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup       9990 2018-09-28 12:45 /data/msd/audio/attributes/msd-mvd-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup       1390 2018-09-28 12:45 /data/msd/audio/attributes/msd-rh-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup      34913 2018-09-28 12:45 /data/msd/audio/attributes/msd-rp-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup       3942 2018-09-28 12:45 /data/msd/audio/attributes/msd-ssd-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup       9990 2018-09-28 12:45 /data/msd/audio/attributes/msd-trh-v1.0.attributes.csv
#-rw-r--r--   8 hadoop supergroup      28313 2018-09-28 12:45 /data/msd/audio/attributes/msd-tssd-v1.0.attributes.csv
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:50 /data/msd/audio/features
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv
#-rw-r--r--   8 hadoop supergroup    8635110 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup    8636689 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup    8632696 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup    8635186 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup    8635805 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup    8632126 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup    8636623 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup    8259803 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv
#-rw-r--r--   8 hadoop supergroup    6995606 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup    6995215 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup    6993977 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup    6994647 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup    6994518 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup    6994607 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup    6993815 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup    6693846 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-lpc-all-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv
#-rw-r--r--   8 hadoop supergroup    4719497 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup    4719887 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup    4718608 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup    4718547 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup    4718096 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup    4718231 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup    4719707 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup    4516529 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-methods-of-moments-all-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv
#-rw-r--r--   8 hadoop supergroup    9325048 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup    9327316 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup    9324074 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup    9324094 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup    9324537 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup    9324133 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup    9322969 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup    8925237 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-mfcc-all-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv
#-rw-r--r--   8 hadoop supergroup    6735834 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup    6737774 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup    6736351 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup    6734170 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup    6735486 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup    6736038 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup    6735459 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup    6445696 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-all-all-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv
#-rw-r--r--   8 hadoop supergroup    6735834 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup    6737774 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup    6736351 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup    6734170 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup    6735486 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup    6736038 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup    6735459 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup    6445696 2018-09-28 12:45 /data/msd/audio/features/msd-jmir-spectral-derivatives-all-all-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:45 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv
#-rw-r--r--   8 hadoop supergroup   54299383 2018-09-28 12:45 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup   54298485 2018-09-28 12:45 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup   54300285 2018-09-28 12:45 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup   54290182 2018-09-28 12:45 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup   54301920 2018-09-28 12:45 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup   54295407 2018-09-28 12:45 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup   54302443 2018-09-28 12:45 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup   52118685 2018-09-28 12:45 /data/msd/audio/features/msd-marsyas-timbral-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:46 /data/msd/audio/features/msd-mvd-v1.0.csv
#-rw-r--r--   8 hadoop supergroup  173955438 2018-09-28 12:46 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup  174056749 2018-09-28 12:46 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup  173994957 2018-09-28 12:46 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup  173966435 2018-09-28 12:46 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup  174019074 2018-09-28 12:46 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup  174009495 2018-09-28 12:46 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup  174030134 2018-09-28 12:46 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup  165959998 2018-09-28 12:46 /data/msd/audio/features/msd-mvd-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:46 /data/msd/audio/features/msd-rh-v1.0.csv
#-rw-r--r--   8 hadoop supergroup   31676800 2018-09-28 12:46 /data/msd/audio/features/msd-rh-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup   31685782 2018-09-28 12:46 /data/msd/audio/features/msd-rh-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup   31683459 2018-09-28 12:46 /data/msd/audio/features/msd-rh-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup   31681284 2018-09-28 12:46 /data/msd/audio/features/msd-rh-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup   31684341 2018-09-28 12:46 /data/msd/audio/features/msd-rh-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup   31685148 2018-09-28 12:46 /data/msd/audio/features/msd-rh-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup   31685069 2018-09-28 12:46 /data/msd/audio/features/msd-rh-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup   30212674 2018-09-28 12:46 /data/msd/audio/features/msd-rh-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:48 /data/msd/audio/features/msd-rp-v1.0.csv
#-rw-r--r--   8 hadoop supergroup  544118501 2018-09-28 12:47 /data/msd/audio/features/msd-rp-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup  544397106 2018-09-28 12:47 /data/msd/audio/features/msd-rp-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup  544256289 2018-09-28 12:47 /data/msd/audio/features/msd-rp-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup  544381287 2018-09-28 12:47 /data/msd/audio/features/msd-rp-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup  544387901 2018-09-28 12:48 /data/msd/audio/features/msd-rp-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup  544352212 2018-09-28 12:48 /data/msd/audio/features/msd-rp-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup  544333839 2018-09-28 12:48 /data/msd/audio/features/msd-rp-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup  519075826 2018-09-28 12:48 /data/msd/audio/features/msd-rp-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:49 /data/msd/audio/features/msd-ssd-v1.0.csv
#-rw-r--r--   8 hadoop supergroup   84441900 2018-09-28 12:49 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup   84463212 2018-09-28 12:49 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup   84453926 2018-09-28 12:49 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup   84452507 2018-09-28 12:49 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup   84457200 2018-09-28 12:49 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup   84464852 2018-09-28 12:49 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup   84461435 2018-09-28 12:49 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup   80545729 2018-09-28 12:49 /data/msd/audio/features/msd-ssd-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:50 /data/msd/audio/features/msd-trh-v1.0.csv
#-rw-r--r--   8 hadoop supergroup  194396472 2018-09-28 12:49 /data/msd/audio/features/msd-trh-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup  194491877 2018-09-28 12:49 /data/msd/audio/features/msd-trh-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup  194448031 2018-09-28 12:49 /data/msd/audio/features/msd-trh-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup  194383236 2018-09-28 12:49 /data/msd/audio/features/msd-trh-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup  194398466 2018-09-28 12:49 /data/msd/audio/features/msd-trh-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup  194483717 2018-09-28 12:49 /data/msd/audio/features/msd-trh-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup  194497316 2018-09-28 12:49 /data/msd/audio/features/msd-trh-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup  185435893 2018-09-28 12:50 /data/msd/audio/features/msd-trh-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/audio/features/msd-tssd-v1.0.csv
#-rw-r--r--   8 hadoop supergroup  523653885 2018-09-28 12:50 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00000.csv.gz
#-rw-r--r--   8 hadoop supergroup  523973513 2018-09-28 12:50 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00001.csv.gz
#-rw-r--r--   8 hadoop supergroup  523846402 2018-09-28 12:50 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00002.csv.gz
#-rw-r--r--   8 hadoop supergroup  523674195 2018-09-28 12:51 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00003.csv.gz
#-rw-r--r--   8 hadoop supergroup  523778813 2018-09-28 12:51 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00004.csv.gz
#-rw-r--r--   8 hadoop supergroup  524004691 2018-09-28 12:51 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00005.csv.gz
#-rw-r--r--   8 hadoop supergroup  523959080 2018-09-28 12:51 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00006.csv.gz
#-rw-r--r--   8 hadoop supergroup  499578908 2018-09-28 12:52 /data/msd/audio/features/msd-tssd-v1.0.csv/part-00007.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/audio/statistics
#-rw-r--r--   8 hadoop supergroup   42224669 2018-09-28 12:52 /data/msd/audio/statistics/sample_properties.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/genre
#-rw-r--r--   8 hadoop supergroup   11625230 2018-09-28 12:52 /data/msd/genre/msd-MAGD-genreAssignment.tsv
#-rw-r--r--   8 hadoop supergroup    8820054 2018-09-28 12:52 /data/msd/genre/msd-MASD-styleAssignment.tsv
#-rw-r--r--   8 hadoop supergroup   11140605 2018-09-28 12:52 /data/msd/genre/msd-topMAGD-genreAssignment.tsv
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/main
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/main/summary
#-rw-r--r--   8 hadoop supergroup   58658141 2018-09-28 12:52 /data/msd/main/summary/analysis.csv.gz
#-rw-r--r--   8 hadoop supergroup  124211304 2018-09-28 12:52 /data/msd/main/summary/metadata.csv.gz
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/tasteprofile
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/tasteprofile/mismatches
#-rw-r--r--   8 hadoop supergroup      91342 2018-09-28 12:52 /data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt
#-rw-r--r--   8 hadoop supergroup    2026182 2018-09-28 12:52 /data/msd/tasteprofile/mismatches/sid_mismatches.txt
#drwxr-xr-x   - hadoop supergroup          0 2018-09-28 12:52 /data/msd/tasteprofile/triplets.tsv
#-rw-r--r--   8 hadoop supergroup   64020759 2018-09-28 12:52 /data/msd/tasteprofile/triplets.tsv/part-00000.tsv.gz
#-rw-r--r--   8 hadoop supergroup   64038083 2018-09-28 12:52 /data/msd/tasteprofile/triplets.tsv/part-00001.tsv.gz
#-rw-r--r--   8 hadoop supergroup   64077499 2018-09-28 12:52 /data/msd/tasteprofile/triplets.tsv/part-00002.tsv.gz
#-rw-r--r--   8 hadoop supergroup   64102442 2018-09-28 12:52 /data/msd/tasteprofile/triplets.tsv/part-00003.tsv.gz
#-rw-r--r--   8 hadoop supergroup   63998697 2018-09-28 12:52 /data/msd/tasteprofile/triplets.tsv/part-00004.tsv.gz
#-rw-r--r--   8 hadoop supergroup   64049032 2018-09-28 12:52 /data/msd/tasteprofile/triplets.tsv/part-00005.tsv.gz
#-rw-r--r--   8 hadoop supergroup   64064101 2018-09-28 12:52 /data/msd/tasteprofile/triplets.tsv/part-00006.tsv.gz
#-rw-r--r--   8 hadoop supergroup   63788582 2018-09-28 12:52 /data/msd/tasteprofile/triplets.tsv/part-00007.tsv.gz


-------------------------------------------------------------------------------------------------------

# Data types

# From the website , we can check the data type:
# for audio:
# 







#   Check the file blocks
    #eg: 
       # check the blocks that mismatches needs
     hdfs fsck /data/msd/tasteprofile/mismatches -files -blocks
-------------------------------------------------------------------------------------------------------

/data/msd/tasteprofile/mismatches/sid_matches_manually_accepted.txt 91342 bytes, replicated: replication=8, 1 b
0. BP-1043872118-132.181.29.189-1533729513169:blk_1073821916_81092 len=91342 Live_repl=8

/data/msd/tasteprofile/mismatches/sid_mismatches.txt 2026182 bytes, replicated: replication=8, 1 block(s):  OK
0. BP-1043872118-132.181.29.189-1533729513169:blk_1073821917_81093 len=2026182 Live_repl=8


Status: HEALTHY
 Number of data-nodes:  16
 Number of racks:               1
 Total dirs:                    1
 Total symlinks:                0

Replicated Blocks:
 Total size:    2117524 B
 Total files:   2
 Total blocks (validated):      2 (avg. block size 1058762 B)
 Minimally replicated blocks:   2 (100.0 %)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Mis-replicated blocks:         0 (0.0 %)
 Default replication factor:    8
 Average block replication:     8.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)

Erasure Coded Block Groups:
 Total size:    0 B
 Total files:   0
 Total block groups (validated):        0
 Minimally erasure-coded block groups:  0
 Over-erasure-coded block groups:       0
 Under-erasure-coded block groups:      0
 Unsatisfactory placement block groups: 0
 Average block group size:      0.0
 Missing block groups:          0
 Corrupt block groups:          0
 Missing internal blocks:       0
FSCK ended at Mon Oct 08 09:51:48 NZDT 2018 in 0 milliseconds


The filesystem under path '/data/msd/tasteprofile/mismatches' is HEALTHY
----------------------------------------------------------------------------------------------------
# In the directory "/data/msd/tasteprofile/mismatches", it has two files:
      # sid_matches_manually_accepted.txt
      # sid_mismatches.txt

 # and for each file, 1 block is needed 
 #  
  





