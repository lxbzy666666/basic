package valor.bigdata.quickstart.api.table;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import valor.bigdata.quickstart.beans.Person;
import valor.bigdata.quickstart.source.KafkaImpl;

import java.util.Random;

/**
 * 测试table的功能
 * 步骤：
 *  1.创建表的执行环境
 *  2.
 * @author gary
 * @date 2022/3/23 14:44
 */
public class TableApiTest {
    public static void main(String[] args) throws Exception {
        KafkaImpl.environment.setParallelism(1);
        DataStream dataStream = getDataStream(KafkaImpl.getKafkaDataStream());
        // 创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(KafkaImpl.environment);

        // table
        Table table = tableEnvironment.fromDataStream(dataStream);
        Table table1 = table.select("name, age, tag");
        tableEnvironment.toDataStream(table1).print("table");

        // SQL
        String sql = "select name from persons where tag >2";
        tableEnvironment.createTemporaryView("persons", dataStream);
        tableEnvironment.toDataStream(tableEnvironment.sqlQuery(sql)).print("SQL");
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
}
