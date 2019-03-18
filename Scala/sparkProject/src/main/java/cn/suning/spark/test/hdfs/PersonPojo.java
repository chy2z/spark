package cn.suning.spark.test.hdfs;

import java.io.Serializable;

/**
 * @program: scalaspark
 * @description:
 * @author: 18093941
 * @create: 2019-03-11 20:02
 **/
public class PersonPojo  implements Serializable {

    private String name;

    private Integer age;

    public PersonPojo(){

    }

    public PersonPojo(String name, Integer age) {
        this.name = name;
        this.age = age;
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
}
