package valor.bigdata.quickstart.api.window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import valor.bigdata.quickstart.beans.Person;
import valor.bigdata.quickstart.source.KafkaImpl;
import valor.bigdata.quickstart.transform.AggregationTransform;

import java.time.Duration;
import java.util.Properties;
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
    /**
     * 日志框架
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(WindowApiTest.class);
    public static final OutputTag<Person> outputTag = new OutputTag<Person>("late-data") {
    };

    public static void main(String[] args) throws Exception {
        KafkaImpl.environment.setParallelism(1);
        windowTest(getKeyedStream(KafkaImpl.getKafkaDataStream()));
        KafkaImpl.execute();
    }

    /**
     * @param dataStream 返回类型为person对象的数据源
     * @return
     */
    private static DataStream<Person> getKeyedStream(DataStream<String> dataStream) {
        return dataStream
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
                });
    }

    /**
     * windowAll 会将上游算子的所有数据全发送到下游算子的某一个分区子任务里。这会造成资源的很大浪费，几乎不会使用
     * 增量
     *
     * @param keyedStream
     */
    private static void windowTest(DataStream<Person> keyedStream) throws Exception {
        SingleOutputStreamOperator<Integer> dataStream = keyedStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                            @Override
                            public long extractTimestamp(Person element, long recordTimestamp) {
                                return element.getLocalCreateTimeStamps();
                            }
                        }))
                .keyBy(new KeySelector<Person, String>() {
                    @Override
                    public String getKey(Person value) throws Exception {
                        return value.getName();
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                // 可再接收当前水位线前2秒的数据过来
                .allowedLateness(Time.seconds(2))
                // 若还有数据到达完全关闭的窗口，则将数据输出到侧输出流中，再将侧输出流和对应的结果进行合并得到正确的结果
                .sideOutputLateData(outputTag)
                .aggregate(new MyAggregateFunction());
        dataStream.print("dataStream");
        // 获取侧输出流的数据
        DataStream<Person> dataStreamSideOutput = dataStream.getSideOutput(outputTag);
        dataStreamSideOutput.print("personOutputTag");
    }

    /**
     * 自定义实现增量聚合函数 ReduceFunction， 触发时机：**元素每进一次未关闭的窗口就会进行增量运算，但是最终结果是窗口关闭才会输出**
     */
    private static class MyReduceFunction implements ReduceFunction<Person> {

        /**
         * @param value1 上次保存的结果数据
         * @param value2 这次新来的数据
         * @return 返回value1和value2的聚合元素，注意：**此处不能新建元素，只能改变value1或value2的值，然后返回，否则永远value1一直是最开始的那个记录
         * @throws Exception
         */
        @Override
        public Person reduce(Person value1, Person value2) throws Exception {
            LOGGER.info("value1：{}, value2:{}", value1, value2);
            value1.setLocalCreateTimeStamps(value2.getLocalCreateTimeStamps());
            value1.setTag(value2.getTag());
            return value1;
        }
    }

    /**
     * 自定义实现增量聚合函数 AggregateFunction 触发时机：**元素每进一次未关闭的窗口就会进行增量运算，但是最终结果是窗口关闭才会输出**
     */
    private static class MyAggregateFunction implements AggregateFunction<Person, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(Person value, Integer accumulator) {
            LOGGER.info("value:{}", value.toString());
            accumulator += 1;
            return accumulator;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            LOGGER.info("accumulator:{}", accumulator);
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    /**
     * 自定义实现全量聚合函数 WindowFunction  触发时机：**等窗口关闭才进行全量计算，并输出结果。与增量聚合函数相比，拥有更多的信息，包括key, window信息等
     */
    private static class MyWindowFunction implements WindowFunction<Person, Integer, String, TimeWindow> {

        @Override
        public void apply(String s, TimeWindow window, Iterable<Person> input, Collector<Integer> out) throws Exception {
            Integer number = 0;
            for (Person person : input) {
                number++;
            }
            out.collect(number);
        }
    }

}
