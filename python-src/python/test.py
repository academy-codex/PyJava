from py4j.java_gateway import JavaGateway

## UDF
def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
        resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr

print("hello")

convertUDF = org.apache.spark.sql.functions.udf(lambda z: convertCase(z),StringType())

# gateway = JavaGateway()
#
# spark_utils = gateway.entry_point.getSparkUtils()
# spark_utils.showDataFrame()