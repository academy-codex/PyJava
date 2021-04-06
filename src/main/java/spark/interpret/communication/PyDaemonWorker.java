package spark.interpret.communication;

public class PyDaemonWorker implements Runnable{

    String pyScriptPath;

    public PyDaemonWorker(String pyScriptPath) {
        this.pyScriptPath = pyScriptPath;
    }

    @Override
    public void run() {

    }
}
