package cn.itcast.flink.streaming;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.serialization.DeserializationSchema;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Author itcast
 * Date 2022/6/5 15:48
 * Desc TODO
 */
public class GenericTypeDemo {
    public static void main(String[] args) {
/*        Collection<Student> arr = new ArrayList<>();

        arr.add(123);

        Class<? extends DeserializationSchema<>>*/
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Student{
        private String name;
        private int age;

    }
}
