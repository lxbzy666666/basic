package valor.bigdata.quickstart.api.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import valor.bigdata.quickstart.beans.Person;
import valor.bigdata.quickstart.source.KafkaImpl;
import valor.bigdata.quickstart.transform.AggregationTransform;

import java.time.Duration;
import java.util.Random;

/**
 * 窗口操作步骤：
 * 1.定义窗口分配器: countWindow, timeWindow, sessionWindow
 * 2.使用窗口函数
 *
 * @author gary
 * @date 2022/3/17 14:00
 */
public class WindowApiTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(WindowApiTest.class);

    public static void main(String[] args) throws Exception {
        DataStream kafkaDataStream = KafkaImpl.getKafkaDataStream();
        KafkaImpl.environment.setParallelism(1);
        windowTest(kafkaDataStream);
        KafkaImpl.environment.execute();
    }

    /**
     * windowAll 会将上游算子的所有数据全发送到下游算子的某一个分区子任务里。这会造成资源的很大浪费，几乎不会使用
     *
     * @param kafkaDataStream
     */
    private static void windowTest(DataStream<String> kafkaDataStream) {
        KeyedStream<Person, String> keyedStream = kafkaDataStream
                .map(new MapFunction<String, Person>() {
                    @Override
                    public Person map(String value) throws Exception {
                        Person person = new Person();
                        String[] params = value.split(",");
                        person.setName(params[0]);
                        person.setAge(new Random().nextInt());
                        person.setLocalCreateTimeStamps(new Long(params[2]));
                        person.setTag(params[1]);
                        return person;
                    }
                }).keyBy(new KeySelector<Person, String>() {
                    @Override
                    public String getKey(Person value) throws Exception {
                        return value.getName();
                    }
                });
        keyedStream.print("keyedStream");
        keyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .aggregate(new AggregateFunction<Person, Integer, Integer>() {

                    @Override
                    public Integer createAccumulator() {
                        return new Integer(0);
                    }

                    @Override
                    public Integer add(Person value, Integer accumulator) {
                        LOGGER.info("value:{}", value.toString());
                        accumulator += 1;
                        return accumulator ;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        LOGGER.info("accumulator:{}", accumulator);
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return null;
                    }
                })
                .print("window");
    }
}
