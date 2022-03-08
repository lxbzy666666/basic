package valor.bigdata.quickstart;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import valor.bigdata.quickstart.beans.Person;
import java.util.Arrays;
import java.util.Date;
import java.util.List;


/**
 * TODO 读取POJO对象作为Source
 */
public class QuickSource {
    /**
     * 一个流执行环境
     */
    private static final StreamExecutionEnvironment ENV = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {
        DataStream<Person> dataStream = getSourceFromBeans(ENV);
        dataStream.print();
        ENV.execute();
    }

    private static DataStream<Person> getSourceFromBeans(StreamExecutionEnvironment environment) {
        List<Person> persons = Arrays.asList(
                new Person("张三", 22, new Date().getTime()),
                new Person("李四", 22, new Date().getTime()),
                new Person("王五", 22, new Date().getTime()),
                new Person("gary", 22, new Date().getTime())
        );
        return environment.fromCollection(persons);
    }
}
