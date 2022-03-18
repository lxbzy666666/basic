package valor.bigdata.quickstart.sink;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import valor.bigdata.quickstart.beans.Person;
import valor.bigdata.quickstart.source.Sources;

/**
 * @author gary
 * @date 2022/3/11 15:30
 */
public class Sinks {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = Sources.getEnvironment();
        environment.execute();
    }

    /**
     * TODO 将数据写入kafka在中
     */
    private static void kafkaSinkTest() {

    }
}
