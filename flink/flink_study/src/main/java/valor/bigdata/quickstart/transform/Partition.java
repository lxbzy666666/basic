package valor.bigdata.quickstart.transform;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import valor.bigdata.quickstart.beans.Person;
import valor.bigdata.quickstart.source.Sources;

/**
 * 测试数据在算子之间的传送模式：
 *  一对一模式：类似于数据之间是窄依赖，即算子之间为一对一得关系
 *  重新分发模式：类似于数据之间是宽依赖
 *
 *  注意：算子链 = 一对一模式 + 两个算子并行度一致
 * @author gary
 * @date 2022/3/11 14:26
 */
public class Partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = Sources.getEnvironment();
        rescaleTest();
        streamExecutionEnvironment.execute();
    }

    /**
     * 场景：当前后两个算子并行度一致但是没法形成算子链时可以使用。
     * 作用：同一分区的数据只会进入到下游算子的同一分区。
     */
    private static void forwardTest() {
        DataStream<Person> dataStream = Sources.getDataStream();
        dataStream.forward().print("forwardTest");
    }

    /**
     * 将数据随机发送到下游的算子分区中
     */
    private static void shuffleTest() {
        DataStream<Person> dataStream = Sources.getDataStream();
        dataStream.shuffle().print("forwardTest");
    }

    /**
     * 将数据往下游算子分区中都发一份，类似于广播
     */
    private static void BroadcastTest() {
        DataStream<Person> dataStream = Sources.getDataStream();
        dataStream.broadcast().print("forwardTest");
    }

    /**
     * 将数据按照轮询方式发送到下游的算子分区中
     */
    private static void rebalanceTest() {
        DataStream<Person> dataStream = Sources.getDataStream();
        dataStream.rebalance().print("rebalanceTest");
    }

    /**
     * 将数据分组（一部分数据走一部分分区），然后再轮询（数据在对应分区内轮询）
     */
    private static void rescaleTest() {
        DataStream<Person> dataStream = Sources.getDataStream();
        dataStream.rescale().print("rescaleTest");
    }

    /**
     * 将数据全发往下一个算子得的一个分区
     */
    private static void globalTest() {
        DataStream<Person> dataStream = Sources.getDataStream();
        dataStream.global().print("globalTest");
    }

}
