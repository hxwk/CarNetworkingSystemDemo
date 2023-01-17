package cn.itcast.flink.streaming;

import java.util.ArrayList;
import java.util.List;

/**
 * Author itcast
 * Date 2022/6/12 12:03
 * Desc TODO
 */
public class JoinDemo {
    public static void main(String[] args) {
        List<String> names = new ArrayList<>();
        names.add("张三");
        names.add("王五");
        names.add("jack");

        System.out.println(String.join("~",names));
    }
}
