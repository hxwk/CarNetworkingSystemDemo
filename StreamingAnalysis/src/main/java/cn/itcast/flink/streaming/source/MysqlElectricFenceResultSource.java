package cn.itcast.flink.streaming.source;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Author itcast
 * Date 2022/6/10 11:04
 * Desc 需求 - 读取mysql中的结果表用于与窗口内的数据流进行 connect 关联操作
 * 将结果集保存到 mysql
 */
public class MysqlElectricFenceResultSource extends RichSourceFunction<HashMap<String, Long>> {

    //定义参数
    private Connection conn;
    private Statement statement;
    //定义用于循环读写数据的变量
    private volatile boolean isRunning = true;

    //1.重写 open 方法，初始化连接
    @Override
    public void open(Configuration parameters) throws Exception {
        //加载本地参数
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        Class.forName(params.get("jdbc.driver"));
        //连接串
        conn = DriverManager.getConnection(
                params.get("jdbc.url"),
                params.get("jdbc.user"),
                params.get("jdbc.password"));
        //创建statement
        statement = conn.createStatement();
    }

    //3.重写 run 方法 获取出来vin 和 id 封装成map并返回
    @Override
    public void run(SourceContext<HashMap<String, Long>> ctx) throws Exception {
        String sql = "select vin, min(id) as id " +
                "from vehicle_networking.electric_fence " +
                "where inTime is not null and outTime is null " +
                "GROUP BY vin";
        while (isRunning) {
            //定义返回类型
            HashMap<String, Long> electricFence = new HashMap<>();
            //1.1 编写sql "select vin, min(id) id from ;"
            ResultSet rs = statement.executeQuery(sql);
            while (rs.next()) {
                electricFence.put(rs.getString("vin"), rs.getLong("id"));
            }
            //返回数据
            ctx.collect(electricFence);
            //休眠时间
            TimeUnit.SECONDS.sleep(5 * 60 * 1000);
        }
    }

    //4.重写 cancel 方法
    @Override
    public void cancel() {
        isRunning = false;
    }

    //2.重写 close 方法
    @Override
    public void close() throws Exception {
        if(!statement.isClosed()) statement.close();
        if(!conn.isClosed()) conn.close();
    }
}
