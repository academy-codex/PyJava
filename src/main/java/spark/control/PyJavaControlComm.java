package spark.control;

import java.util.List;
import java.util.Map;

public interface PyJavaControlComm {
    Map invoke(Map<String, String> dataSetMap);
}
