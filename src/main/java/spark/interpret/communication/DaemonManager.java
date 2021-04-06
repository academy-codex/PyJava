package spark.interpret.communication;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DaemonManager {
    ExecutorService executorService;
    static boolean completed = false;

    public DaemonManager() {
        executorService = Executors.newFixedThreadPool(5);
    }

    void executePythonDaemonTask(String pyScriptPath) {
        executorService.execute(new PyDaemonWorker(pyScriptPath));
    }
}
