package valor.bigdata.quickstart.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import valor.bigdata.quickstart.beans.Person;

import java.util.Arrays;
import java.util.List;

/**
 * @author gary
 * @date 2022/3/9 13:34
 */
public class Sources {

    private static final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

    public static StreamExecutionEnvironment getEnvironment() {
        return environment;
    }
    public static void execute() throws Exception {
        environment.execute();
    }

    public static DataStream<Person> getDataStream() {
        List<Person> persons = Arrays.asList(
                new Person("张三", 22, System.currentTimeMillis()),
                new Person("李四", 23,System.currentTimeMillis()),
                new Person("王五", 24, System.currentTimeMillis()),
                new Person("gary", 25, System.currentTimeMillis()),
                new Person("gary", 29, System.currentTimeMillis()),
                new Person("gary2", 18, System.currentTimeMillis()),
                new Person("gary3", 19, System.currentTimeMillis())
        );
        return environment.fromCollection(persons);
    }
}
