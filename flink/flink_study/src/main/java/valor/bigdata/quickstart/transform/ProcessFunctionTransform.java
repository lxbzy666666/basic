package valor.bigdata.quickstart.transform;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import valor.bigdata.quickstart.api.state.StateTest;
import valor.bigdata.quickstart.beans.Person;
import valor.bigdata.quickstart.source.KafkaImpl;

import java.time.Duration;
import java.util.Random;

/**
 * @author gary
 * @date 2022/3/22 15:45
 */
public class ProcessFunctionTransform {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessFunctionTransform.class);

    public static void main(String[] args) throws Exception {
        KafkaImpl.environment.setParallelism(3);
        DataStream dataStream = getDataStream(KafkaImpl.getKafkaDataStream());
        dataStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Person>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Person>() {
                            @Override
                            public long extractTimestamp(Person element, long recordTimestamp) {
                                return element.getLocalCreateTimeStamps();
                            }
                        }))
                .keyBy(new KeySelector<Person, String>() {
                    @Override
                    public String getKey(Person value) {
                        return value.getName();
                    }
                }).process(new MyKeyedProcessFunction());
        KafkaImpl.execute();
    }

    /**
     * @param dataStream 返回类型为person对象的数据源
     * @return
     */
    private static DataStream<Person> getDataStream(DataStream<String> dataStream) {
        return dataStream
                .map(new MyRichMapFunction());
    }

    private static class MyKeyedProcessFunction extends KeyedProcessFunction<String, Person, Person> {

        private ValueState<Long> tsTime = null;

        @Override
        public void processElement(Person value, Context ctx, Collector<Person> out) throws Exception {
            // 获取当前元素
//            LOGGER.info("person is {}", value);
//            LOGGER.info("CurrentKey is {}", ctx.getCurrentKey());
//            LOGGER.info("timestamp is {}", ctx.timestamp());
            LOGGER.info("currentWatermark is {}", ctx.timerService().currentWatermark());
            Long value1 = ctx.timerService().currentProcessingTime() + 1000;
            tsTime.update(value1);
            LOGGER.info("tsTime is {}", tsTime.value());
            ctx.timerService().registerProcessingTimeTimer(tsTime.value());
            ctx.timerService().deleteProcessingTimeTimer(tsTime.value());
        }

        /**
         * 调用时间触发器，类似于闹钟
         * @param timestamp 触发的时间
         * @param ctx
         * @param out
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Person> out) {
            LOGGER.info("timestamp: {}", timestamp);
        }

        /** 注册状态
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            tsTime = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ps-time", Long.class));
        }

        /** 清楚状态
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            tsTime.clear();
        }
    }

    private static class MyRichMapFunction extends RichMapFunction<String, Person> {

        @Override
        public Person map(String value) throws Exception {
            Person person = new Person();
            String[] params = value.split(",");
            person.setName(params[0]);
            person.setAge(new Random().nextInt());
            person.setLocalCreateTimeStamps(new Long(params[2]));
            person.setTag(params[1]);
            LOGGER.info("person is {}", person);
            return person;
        }
    }
}
