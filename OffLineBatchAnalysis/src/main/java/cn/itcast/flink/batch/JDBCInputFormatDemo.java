package cn.itcast.flink.batch;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

/**
 * Author itcast
 * Desc 通过JDBC方式读取mysql中的数据并将其打印输出
 */
public class JDBCInputFormatDemo {
    public static void main(String[] args) throws Exception {
        //1.获取批处理环境 ExecutionEnvironment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        //2.创建 JDBCInputFormat 读取的数据的格式、字段、类型
        //加载读取数据源的 url 用户名、密码、SQL语句、驱动等
        JDBCInputFormat jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(ConfigLoader.get("jdbc.driver"))
                .setDBUrl(ConfigLoader.get("jdbc.url"))
                .setUsername(ConfigLoader.get("jdbc.user"))
                .setPassword(ConfigLoader.get("jdbc.password"))
                .setQuery("select name,gender,age from t_student")
                .setRowTypeInfo(new RowTypeInfo(Types.STRING,Types.STRING,Types.INT))
                .setFetchSize(2)
                .finish();
        //3.添加mysql的输入格式
        DataSource<Row> source = env.createInput(jdbcInputFormat);
        //4.打印输出
        source.printToErr();
    }
}