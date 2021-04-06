package spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import py4j.GatewayServer;
import spark.control.PyJavaControlComm;
import spark.interpret.communication.PyDaemonWorker;
import spark.interpret.communication.PythonInterpreter;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class PythonSparkInterfaceEntryPoint {
    static Map<String, String> dataFrames;

    static SparkUtils sparkUtils;

    private static PythonInterpreter pythonInterpreter;

    public PythonInterpreter getPythonInterpreter() {
        return pythonInterpreter;
    }

    public void setPythonInterpreter(PythonInterpreter pythonInterpreter) {
        this.pythonInterpreter = pythonInterpreter;
    }

    public void testControlReturn(String outputDataframe) {
        System.out.println("DF: " + outputDataframe);
        Dataset<Row> returnedDf = sparkUtils.getSqlContext().sql("SELECT * FROM " + outputDataframe);
        returnedDf.show();
    }

    public void interpret(String pythonCode) {
        Map<String, String> dfMap = new HashMap<>();
        dfMap.put("df1","testTable");
        Object returnValue = this.pythonInterpreter.callPython(dfMap);
        System.out.println(returnValue);
    }

    public static void runFromJava() {
        Map<String, String> dfMap = new HashMap<>();
        dfMap.put("df1","testTable");
        Map<String, String> o = pythonInterpreter.callPython(dfMap);
        System.out.println("Object Recevied!");

        System.out.println("Map: " + o);

        Dataset<Row> output1 = sparkUtils.getSqlContext().sql("SELECT * FROM " + o.get("output1"));
        output1.show();

        System.out.println("Count: " + output1.count());
//        pythonCode = "gateway.shutdown()";
//        this.pythonInterpreter.callPython(pythonCode);


    }

    public void sampleJavaPy() {

    }

    PythonSparkInterfaceEntryPoint() {
        sparkUtils = new SparkUtils();
        sparkUtils.initTestDataFrame();
    }

    private static void initSpark() {
        sparkUtils = new SparkUtils();
        sparkUtils.initTestDataFrame();
    }

    public SparkUtils getSparkUtils() {
        return sparkUtils;
    }

    public static void main(String[] args) {
        initSpark();

//        PythonGatewayServerImpl pythonGatewayServerImpl = new PythonGatewayServerImpl(new PyJavaControlEntryPoint());
//        pythonGatewayServerImpl.init();

        GatewayServer gatewayServer = new GatewayServer(new PythonSparkInterfaceEntryPoint(), 25445);
        gatewayServer.start();
        System.out.println("Gateway Server Started!");

        Scanner scanner = new Scanner(System.in);
        scanner.next();

        runFromJava();
//        try (Jep interp = new SharedInterpreter()) {
//            interp.eval("from java.lang import System");
//            interp.exec("s = 'Hello World'");
//            interp.exec("System.out.println(s)");
//            interp.exec("print(s)");
//            interp.exec("print(s[1:-1])");
//        } catch (JepException e) {
//            e.printStackTrace();
//        }

    }

    void execute(PyJavaControlComm pyJavaControlComm) {
        pyJavaControlComm.invoke(dataFrames);
    }
}
