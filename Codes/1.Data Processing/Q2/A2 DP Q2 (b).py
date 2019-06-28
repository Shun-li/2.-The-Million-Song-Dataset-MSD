# Audio attributes to schema example

import json
from pyspark.sql.types import *

DTYPES_MAPPING = {
    "real": DoubleType(),
    "string": StringType(),
    "NUMERIC": DoubleType(),
    "STRING": StringType(),
}

def attributes_to_schema(hdfs_path, dtypes_mapping=DTYPES_MAPPING):

    rows = spark.read.csv(hdfs_path).collect() # collect will take a dataframe to a python [Row(...), ...]
    structfields = []
    for row in rows:
        colname = row[0]
        dtypestring = row[1]
        structfields.append(StructField(colname, dtypes_mapping[dtypestring], True))

    return StructType(structfields)


if __name__ == "__main__":

    # Example

    attributes_hdfs_path = "hdfs:///data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv"
    features_hdfs_path = "hdfs:///data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv"

    schema = attributes_to_schema(attributes_hdfs_path)
    data = spark.read.schema(schema).csv(features_hdfs_path)

    print(json.dumps(data.head().asDict(), indent=2))

    # For loop to iterate through all audio datasets

    files = [
        "/data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-lpc-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-methods-of-moments-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-mfcc-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-spectral-all-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-jmir-spectral-derivatives-all-all-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-marsyas-timbral-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-mvd-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-rh-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-rp-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-ssd-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-trh-v1.0.attributes.csv",
        "/data/msd/audio/attributes/msd-tssd-v1.0.attributes.csv"
    ]
    import re

    for attributes_file in files:
        features_file = re.sub("\.attributes", "", attributes_file)
        features_file = re.sub("attributes", "features", features_file)
        
        print(attributes_file)
        print(features_file)

        schema = attributes_to_schema(attributes_file)
        data = spark.read.schema(schema).csv(features_file).limit(1)

        print(json.dumps(data.head().asDict(), indent=2))
        print("")


--------------------------------------------------------------------------------------------------------
#using the names and types in audio/attributes to define the schemas of audio/features



hdfs dfs -text hdfs:///data/msd/audio/attributes/msd-jmir-area-of-moments-all-v1.0.attributes.csv | head -30

Area_Method_of_Moments_Overall_Standard_Deviation_1,real
Area_Method_of_Moments_Overall_Standard_Deviation_2,real
Area_Method_of_Moments_Overall_Standard_Deviation_3,real
Area_Method_of_Moments_Overall_Standard_Deviation_4,real
Area_Method_of_Moments_Overall_Standard_Deviation_5,real
Area_Method_of_Moments_Overall_Standard_Deviation_6,real
Area_Method_of_Moments_Overall_Standard_Deviation_7,real
Area_Method_of_Moments_Overall_Standard_Deviation_8,real
Area_Method_of_Moments_Overall_Standard_Deviation_9,real
Area_Method_of_Moments_Overall_Standard_Deviation_10,real
Area_Method_of_Moments_Overall_Average_1,real
Area_Method_of_Moments_Overall_Average_2,real
Area_Method_of_Moments_Overall_Average_3,real
Area_Method_of_Moments_Overall_Average_4,real
Area_Method_of_Moments_Overall_Average_5,real
Area_Method_of_Moments_Overall_Average_6,real
Area_Method_of_Moments_Overall_Average_7,real
Area_Method_of_Moments_Overall_Average_8,real
Area_Method_of_Moments_Overall_Average_9,real
Area_Method_of_Moments_Overall_Average_10,real
MSD_TRACKID,string

# totally 21 rows.



hdfs dfs -text hdfs:///data/msd/audio/features/msd-jmir-area-of-moments-all-v1.0.csv/part-00000.csv.gz | head -1
1.431,6713.0,52600.0,160600000.0,1264000000.0,9943000000.0,7.086e+12,11400000000.0,89730000000.0,3.465e+15,5.252,11580.0,90080.0,-179100000.0,-1396000000.0,-10870000000.0,6.236e+12,12580000000.0,98020000000.0,2.97e+15,'TRMMMYQ128F932D901'
#totally 21 columns.


 