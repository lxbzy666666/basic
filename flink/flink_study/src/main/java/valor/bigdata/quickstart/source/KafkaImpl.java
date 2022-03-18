package valor.bigdata.quickstart.source;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import valor.bigdata.quickstart.beans.Person;

import java.time.Duration;

/**
 * 将kafka作为source的来源
 * @author gary
 * @date 2022/3/17 14:07
 */
public class KafkaImpl extends SourceAbstract{
    private static String bootstrapServers = "node1:9092,node2:9092,node3:9092";
    private static String topic = "flink_test";
    private static String groupId = "flink_group";


    /**
     * 配置kafka连接信息
     * @return 配置了kafka连接对象的源对象
     */
    private static KafkaSource<String> getKafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers("node1:9092,node2:9092,node3:9092")
                .setTopics("flink_test")
                .setGroupId("flink")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static DataStreamSource getKafkaDataStream () {
        return environment.fromSource(getKafkaSource(), WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        return new Long(element.split(",")[2]);
                    }
                }), "kafka_source");
    }

}
