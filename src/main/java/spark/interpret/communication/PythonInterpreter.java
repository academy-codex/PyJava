package spark.interpret.communication;

import java.util.Map;

public interface PythonInterpreter {
    public Map callPython(Map<String, String> dfMap);
}
