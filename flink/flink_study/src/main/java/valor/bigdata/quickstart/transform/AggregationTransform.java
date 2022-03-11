package valor.bigdata.quickstart.transform;

import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import valor.bigdata.quickstart.beans.Person;
import valor.bigdata.quickstart.source.Sources;

/**
 * TODO 聚合算子：即类似于SQL中的分组后的一系列操作。只有对流进行分组后，才能进行reduce,sum等操作
 * @author gary
 * @date 2022/3/9 14:42
 */
public class AggregationTransform {
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationTransform.class);

    public static void main(String[] args) throws Exception {
        DataStream<Person> personDataStream = Sources.getDataStream();
        KeyedStream keyByPersonDataStream = keyBy(personDataStream);
        richFunctionTest(personDataStream);
        Sources.execute();
    }

    /**
     * KeyedStream是将流进行分组后得到的，在分布式环境下，相同的key将存在同一个流中，即在同一个task slot上执行
     * @param dataStream 未分组的数据流
     * @return 返回一个KeyedStream
     */
    private static KeyedStream keyBy(DataStream dataStream) {
        return dataStream.keyBy((KeySelector<Person, String>) person -> person.getName());
    }

    /**
     * 滚动获取按key分组的流中，**该列上的最大值/最小值，其他列的值按照最先的值，不会更新**
     * @param keyedStream 被分组的流
     */
    private static void maxAndMinTest(KeyedStream keyedStream) {
        keyedStream.max("age").print("max_Test");
        keyedStream.min("age").print("min_Test");
    }

    /**
     * 分组获取最大/最小值的**那条记录**
     * @param keyedStream 被分组的流
     */
    private static void maxByAndMinByTest(KeyedStream keyedStream) {
        keyedStream.maxBy("age").print("maxBy_Test");
//        keyedStream.minBy("age").print("minBy_Test");
    }

    /**
     *分组获取最新的tag,以前的name,age,时间戳
     * @param keyedStream 被分组的流
     */
    private static void reduceTest(KeyedStream keyedStream) {
        keyedStream.reduce((ReduceFunction<Person>) (value1, value2) -> new Person(value1.getName(),value2.getAge(), value2.getLocalCreateTimeStamps(),value1.getTag()))
        .print("reduceTest");
    }

    /**
     * 将单条流转换为多条流，类似于将流中的数据打一个标签,方便后续检出
     */
    private static SingleOutputStreamOperator multipleStreamTest(DataStream dataStream) {
        OutputTag<Person> highTag = new OutputTag<>("高版本", TypeInformation.of(Person.class));
        OutputTag<Person> lowTag = new OutputTag<>("低版本", TypeInformation.of(Person.class));
        SingleOutputStreamOperator streamOperator = dataStream.process(new ProcessFunction<Person, Person>() {
            @Override
            public void processElement(Person value, ProcessFunction.Context ctx, Collector out) throws Exception {
                if (value.getTag().contains("1")) {
                    ctx.output(lowTag, value);
                } else {
                    ctx.output(highTag, value);
                }
            }
        });
        return streamOperator;
    }

    /**
     * 将两条不同的流合并为同一条流
     */
    private static void connectStreamTest(DataStream dataStream) {
        OutputTag<Person> highTag = new OutputTag<>("高版本", TypeInformation.of(Person.class));
        OutputTag<Person> lowTag = new OutputTag<>("低版本", TypeInformation.of(Person.class));
        SingleOutputStreamOperator streamOperator = multipleStreamTest(dataStream);
        DataStream highStream = streamOperator.getSideOutput(highTag);
        DataStream lowStream = streamOperator.getSideOutput(lowTag);

        // 将highStream转换为二元组（name, 高版本）
        DataStream<Tuple2<String, String>> highStreamMap = highStream.map(new MapFunction<Person, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(Person value) throws Exception {
                return new Tuple2<>(value.getName(), "高版本");
            }
        });

        // 将highStreamMap与lowStream合并为
        ConnectedStreams connectedStreams = highStreamMap.connect(lowStream);
        connectedStreams.map(new CoMapFunction<Tuple2<String, String>, Person,Object>() {
            @Override
            public Object map1(Tuple2<String, String> value) throws Exception {
                return "name is :" + value.f0 + " tag is :" + value.f1;
            }

            @Override
            public Object map2(Person value) throws Exception {
                return "name is :" + value.getName() + " tag is :" + value.getTag();
            }
        }).print("connect stream ");
    }

    /**
     * 将多条流合并为一个流，要求每个流的结构需要一样
     * @param dataStream
     */
    private static void unionStreamTest(DataStream dataStream) {
        OutputTag<Person> highTag = new OutputTag<>("高版本", TypeInformation.of(Person.class));
        OutputTag<Person> lowTag = new OutputTag<>("低版本", TypeInformation.of(Person.class));
        SingleOutputStreamOperator streamOperator = multipleStreamTest(dataStream);
        DataStream highStream = streamOperator.getSideOutput(highTag);
        DataStream lowStream = streamOperator.getSideOutput(lowTag);
        highStream.union(lowStream).print("unionStreamTest");
    }

    /** 富函数用于扩展Function接口，可以在执行dag之前初始化针对每个slot的全局资源，并且还有生命周期
     * @param dataStream
     */
    private static void richFunctionTest (DataStream dataStream) {
        dataStream.map(new RichMapFunction<Person, Tuple2<String, Integer>>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                LOGGER.warn("this is initial operator");
            }

            @Override
            public void close() throws Exception {
                LOGGER.warn("this is close operator");
            }

            @Override
            public Tuple2<String, Integer> map(Person value) {
                return new Tuple2<>(value.getName(), getRuntimeContext().getIndexOfThisSubtask());
            }
        }).print("richFunctionTest");
    }
}
