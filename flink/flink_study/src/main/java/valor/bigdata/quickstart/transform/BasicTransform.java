package valor.bigdata.quickstart.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import valor.bigdata.quickstart.beans.Person;
import valor.bigdata.quickstart.source.Sources;

/**
 * TODO 学习基本transform operators 的用法
 * @author gary
 * @date 2022/3/9 13:32
 */
public class BasicTransform {
    public static void main(String[] args) throws Exception {
        DataStream<Person> personDataStream = Sources.getDataStream();
        mapTest(personDataStream);
        flatMapTest(personDataStream);
        filterTest(personDataStream);
        Sources.execute();
    }

    /**
     * 将对象转为2元组（对象，对象名）
     * @param dataStream 流
     */
    private static void mapTest(DataStream dataStream) {
        dataStream.map(new MapFunction<Person, Tuple2<Person, String>>() {
            @Override
            public Tuple2<Person, String> map(Person person) throws Exception {
                return new Tuple2<>(person, person.getName());
            }
        }).print("mapTest");
    }

    /**
     * 将对象属性进行拆分
     * @param dataStream
     */
    private static void flatMapTest(DataStream dataStream) {
        dataStream
                .flatMap(new FlatMapFunction<Person, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(Person person, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String s : person.toString().split(",")) {
                            out.collect(new Tuple2<>(s,1));
                        }

                    }
                })
                .print("flatMapTest");
    }

    /**
     * 筛选年龄大于23岁的对象
     */
    private static void filterTest(DataStream dataStream) {
        dataStream.filter((FilterFunction<Person>) value -> value.getAge() > 23).print("filterTest");
    }

}
