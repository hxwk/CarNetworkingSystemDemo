package cn.itcast.flink.streaming.source;

import cn.itcast.flink.streaming.entity.ElectricFenceResultTmp;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Author itcast
 * Date 2022/6/9 16:13
 * Desc 主要实现读取电子围栏数据的核心业务逻辑
 * 将从mysql 中读取的数据封装成一个 HashMap[vin,电子围栏数据]
 * ElectricFenceResultTmp 就是读取数据库的对象
 */
// HashMap<String,ElectricFenceResultTmp>  String : vin , ElectrcFenceResultTmp ： 电子围栏数据
public class MysqlElectricFenceSouce extends RichSourceFunction<HashMap<String, ElectricFenceResultTmp>> {

    //定义参数
    private Connection conn;
    private Statement statement;
    //定义用于循环读写数据的变量
    private volatile boolean isRunning = true;

    /**
     * 加载本地数据参数，连接数据库的参数
     *
     * @param parameters
     * @throws Exception
     */
    //1.重写 open 方法
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

    //3.重写 run 方法
    @Override
    public void run(SourceContext<HashMap<String, ElectricFenceResultTmp>> ctx) throws Exception {
        //3.1 每指定 6个小时 循环读取 mysql 中的电子围栏规则
        while(isRunning){
            //定义 HashMap 对象
            HashMap<String, ElectricFenceResultTmp> electricFence = new HashMap<String, ElectricFenceResultTmp>();
            ResultSet rs = statement.executeQuery(
                    "select vins.vin,setting.id,setting.name,setting.address,setting.radius,setting.longitude,setting.latitude,setting.start_time,setting.end_time " +
                            "from vehicle_networking.electronic_fence_setting setting " +
                            "inner join vehicle_networking.electronic_fence_vins vins on setting.id=vins.setting_id " +
                            "where setting.status=1"
            );
            //遍历执行的结果集并封装对象
            while(rs.next()){
                electricFence.put(
                        rs.getString("vin"),
                        new ElectricFenceResultTmp(
                                rs.getInt("id"),
                                rs.getString("name"),
                                rs.getString("address"),
                                rs.getFloat("radius"),
                                rs.getFloat("longitude"),
                                rs.getFloat("latitude"),
                                rs.getDate("start_time"),
                                rs.getDate("end_time")
                        )
                );
            }
            //将结果集输出
            ctx.collect(electricFence);
            //3.2 收集 electricFenceResult 指定休眠时间
            TimeUnit.HOURS.sleep(6);
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
        //2.1 关闭 statement 和 conn
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            if (!conn.isClosed()) conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
