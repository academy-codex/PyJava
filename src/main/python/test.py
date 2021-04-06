from py4j.java_gateway import JavaGateway
import pyspark
from pyspark import SparkSession

## UDF
def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
        resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr

print("hello")



# gateway = JavaGateway()
#
# spark_utils = gateway.entry_point.getSparkUtils()
# spark_utils.showDataFrame()