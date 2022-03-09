package valor.bigdata.quickstart.transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import valor.bigdata.quickstart.beans.Person;
import valor.bigdata.quickstart.source.Sources;

/**
 * TODO 聚合算子：即类似于SQL中的分组后的一系列操作。只有对流进行分组后，才能进行reduce,sum等操作
 * @author gary
 * @date 2022/3/9 14:42
 */
public class AggregationTransform {
    public static void main(String[] args) throws Exception {
        DataStream<Person> personDataStream = Sources.getDataStream();
        KeyedStream keyByPersonDataStream = keyBy(personDataStream);
        keyByPersonDataStream.print("keyByTest");

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
}
