package spark.control;

import java.util.HashMap;
import java.util.Map;

public class PyJavaControlEntryPoint {
    static Map<String, String> dataFrames;

    public PyJavaControlEntryPoint() {
        dataFrames = new HashMap<>();
    }

    void execute(PyJavaControlComm pyJavaControlComm) {
        pyJavaControlComm.invoke(dataFrames);
    }
}
