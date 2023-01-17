package cn.itcast.flink.batch;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Author itcast
 * Date 2022/6/12 16:14
 * Desc 此类中主要实现将数据写入到 mysql 中的操作
 * 需要使用到 JDBCOutputFormat 对象
 * 此对象需要使用到 JDBCOutputFormatBuilder 构建者对象实现
 */
public class OutputFormatDemo {
    public static void main(String[] args) throws Exception {
        //1.创建批执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.生成测试数据，由于插入数据需要是Row格式，提前设置为Row
        Collection<Row> rows = new ArrayList<>();
        Row row1 = new Row(3);
        row1.setField(0, "zhangsan");
        row1.setField(1, "male");
        row1.setField(2, 25);

        rows.add(row1);

        Row row2 = new Row(3);
        row2.setField(0, "wangwu");
        row2.setField(1, "female");
        row2.setField(2, 22);

        rows.add(row2);

        DataSource<Row> source = env.fromCollection(rows);
        //3.将数据写出到 mysql 数据库
        JDBCOutputFormat outputFormat = JDBCOutputFormat.buildJDBCOutputFormat()
                .setQuery("insert into t_student values(?,?,?)")
                .setDrivername(ConfigLoader.get("jdbc.driver"))
                .setDBUrl(ConfigLoader.get("jdbc.url"))
                .setUsername(ConfigLoader.get("jdbc.user"))
                .setPassword(ConfigLoader.get("jdbc.password"))
                .setBatchInterval(200)
                .finish();

        //4.执行输出
        source.output(outputFormat);
        //执行程序
        env.execute();
    }
}
