package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {
    SparkSession spark = SparkSession.builder()
            .master("local[*]")
            .appName("SimpleApplication")
            .getOrCreate();

    static Dataset<Row> testDf;

    static Dataset<Row> outputDf;

    public void showDataFrame() {
        if (null == testDf)
            testDf = initTestDataFrame();

        testDf.show();
    }

    public void setOutputDf(Dataset<Row> dataset) {
        outputDf = dataset;

        System.out.println("Received an output dataframe");
        outputDf.show();
    }

    public SQLContext getSqlContext() {
        return spark.sqlContext();
    }

    public SparkSession getSparkSession() {
        return spark;
    }

    public SparkConf getSparkConf() {
        return spark.sparkContext().getConf();
    }

    public JavaSparkContext getSparkContext() {
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        return javaSparkContext;
    }

    public Dataset<Row> getTestDataFrame() {
        return testDf;
    }

    public Dataset<Row> getDf2() {
        return spark.read().option("header","true").csv("C:\\Users\\siddh\\Downloads\\simple-scala-spark-gradle-master\\python-src\\python\\test2.csv");
    }

    public long countRowsInDataFrame() {
        return testDf.count();
    }

    public Dataset<Row> initTestDataFrame() {
        testDf = spark.read().option("header","true").csv("C:\\Users\\siddh\\Downloads\\simple-scala-spark-gradle-master\\python-src\\python\\test_file.csv");
        testDf.registerTempTable("testTable");
        return testDf;
    }
}
