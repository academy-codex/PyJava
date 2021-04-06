package spark;

import org.apache.spark.util.Utils;
import py4j.GatewayServer;

import java.net.InetAddress;

public class PythonGatewayServerImpl {

    Object entryPoint;

    PythonGatewayServerImpl(Object entryPoint) {
        this.entryPoint = entryPoint;
    }

    void init() {
        String secret = "abcd";

        InetAddress localhost = InetAddress.getLoopbackAddress();
        GatewayServer.GatewayServerBuilder builder = new GatewayServer.GatewayServerBuilder()
                .javaPort(0)
                .javaAddress(localhost)
                .entryPoint(entryPoint)
                .callbackClient(GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret);

        if (System.getenv().getOrDefault("_PYSPARK_CREATE_INSECURE_GATEWAY", "0") != "1") {
            builder.authToken(secret);
        } else {
            assert(System.getenv().getOrDefault("SPARK_TESTING", "0") == "1");
        }

        GatewayServer gatewayServer = builder.build();
        gatewayServer.start();

        int boundPort = gatewayServer.getListeningPort();
        if (boundPort == -1) {
            System.out.println("Gateway server failed to bind; exiting");
            System.exit(1);
        } else {
            System.out.println("Started PythonGatewayServer on port "+boundPort);
        }
    }
}
