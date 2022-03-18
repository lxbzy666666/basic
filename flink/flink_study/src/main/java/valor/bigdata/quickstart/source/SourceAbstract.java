package valor.bigdata.quickstart.source;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author gary
 * @date 2022/3/17 14:08
 */
public abstract class SourceAbstract {
    public static final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    public static StreamExecutionEnvironment getEnvironment() {
        return environment;
    }

    public static void execute() throws Exception {
        environment.execute();
    }
}
