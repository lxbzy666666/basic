package valor.bigdata.quickstart.api.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import valor.bigdata.quickstart.api.window.WindowApiTest;
import valor.bigdata.quickstart.beans.Person;
import valor.bigdata.quickstart.source.KafkaImpl;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * 状态测试：常用状态包括算子状态和keyed状态
 * 算子状态：状态只跟算子相关，同一个slot上的不同key享有同一个状态。算子子任务的增加或减少将导致状态的分流和合并。
 *
 * @author gary
 * @date 2022/3/22 10:31
 */
public class StateTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(StateTest.class);

    public static void main(String[] args) throws Exception {

        DataStream dataStream = getDataStream(KafkaImpl.getKafkaDataStream());
//        dataStream.keyBy((KeySelector<Person, String>) value -> value.getName()).map(new MyMapFunction()).print("dataStream");
        dataStream.keyBy((KeySelector<Person, String>) value -> value.getName()).map(new MyRichMapFunction()).print("dataStream");
        KafkaImpl.execute();
    }

    /**
     * @param dataStream 返回类型为person对象的数据源
     * @return
     */
    private static DataStream<Person> getDataStream(DataStream<String> dataStream) {
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
     * 自定义实现mapFunction,同时实现算子状态
     */
    private static class MyMapFunction implements MapFunction<Person, Person>, ListCheckpointed<Person> {

        // 定义一个状态变量，保存第一个数据做为保存的状态
        private Person person = null;

        @Override
        public Person map(Person value) throws Exception {
            if (person == null) {
                person = value;
            }
            return person;
        }

        /**
         * 在checkpoint时将状态进行保存存盘
         * @param checkpointId
         * @param timestamp
         * @return
         * @throws Exception
         */
        @Override
        public List<Person> snapshotState(long checkpointId, long timestamp) throws Exception {
            LOGGER.info("checkpointId:{}, timestamp:{}", checkpointId, timestamp);
            return Collections.singletonList(person);
        }

        /**
         * 在故障重启时使用保存的状态恢复数据
         * @param state
         * @throws Exception
         */
        @Override
        public void restoreState(List<Person> state) throws Exception {
            person = state.get(state.size() - 1);
        }
    }

    private static class MyRichMapFunction extends RichMapFunction<Person, Integer> {

        /**
         * 统计当前key的个数
         */
        private ValueState<Integer> keyCount = null;

        @Override
        public Integer map(Person value) throws Exception {
            Integer value2 = keyCount.value();
            if (value2 == null) {
                LOGGER.info("value state initial start");
                value2 = 0;
            }
            value2 += 1;
            keyCount.update(value2);
            return keyCount.value();
        }

        /**
         * 在open方法中进行状态变量的初始化操作，因为getRuntimeContext需要在集群初始化完成后才能进行使用，在其他地方使用就会报错
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            keyCount = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count", Integer.class));
        }
    }
}
