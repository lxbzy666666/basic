package valor.bigdata.quickstart.beans;

public class Person {
    private String name;
    private Integer age;
    private Long localCreateTimeStamps;
    private String tag;
    public Person() {
    }


    public Person(String name, Integer age, Long localCreateTimeStamps, String tag) {
        this.name = name;
        this.age = age;
        this.localCreateTimeStamps = localCreateTimeStamps;
        this.tag = tag;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public Long getLocalCreateTimeStamps() {
        return localCreateTimeStamps;
    }

    public void setLocalCreateTimeStamps(Long localCreateTimeStamps) {
        this.localCreateTimeStamps = localCreateTimeStamps;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", localCreateTimeStamps=" + localCreateTimeStamps +
                ", tag='" + tag + '\'' +
                '}';
    }
}
