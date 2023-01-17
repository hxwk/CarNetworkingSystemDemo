package cn.itcast.flink.streaming.source;

import cn.itcast.flink.streaming.entity.VehicleInfoModel;
import cn.itcast.flink.streaming.util.DateFormatDefine;
import cn.itcast.flink.streaming.util.DateUtil;
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
 * Date 2022/6/12 15:00
 * Desc 此类主要用于读取 mysql 中的数据车辆静态数据
 */
public class VehicleInfoMysqlSource extends RichSourceFunction<HashMap<String, VehicleInfoModel>> {

    private Connection conn = null;
    private Statement statement = null;
    //取消自动读取数据表数据
    private boolean isRunning = true;

    /**
     * 读取配置文件，并加载mysql的connection
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //获取参数中的连接字符串的全局参数
        ParameterTool params = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        //获取连接驱动
        String driver = params.get("jdbc.driver", "com.mysql.jdbc.Driver");
        Class.forName(driver);
        //获取连接 url
        String url = params.get("jdbc.url");
        //获取用户名和密码
        String username = params.get("jdbc.user");
        String password = params.get("jdbc.password");
        //获取数据库连接
        conn = DriverManager.getConnection(url, username, password);
        //生成 statement
        statement = conn.createStatement();
    }

    /**
     * 读取mysql中的数据并将其封装到 VehicleInfoModel 对象中
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<HashMap<String, VehicleInfoModel>> ctx) throws Exception {
        String sqlQuery = "select t12.vin,t12.series_name,t12.model_name,t12.series_code,t12.model_code,t12.nick_name,t3.sales_date product_date,t4.car_type\n" +
                " from (select t1.vin, t1.series_name, t2.show_name as model_name, t1.series_code,t2.model_code,t2.nick_name,t1.vehicle_id\n" +
                " from vehicle_networking.dcs_vehicles t1 left join vehicle_networking.t_car_type_code t2 on t1.model_code = t2.model_code) t12\n" +
                " left join  (select vehicle_id, max(sales_date) sales_date from vehicle_networking.dcs_sales group by vehicle_id) t3\n" +
                " on t12.vehicle_id = t3.vehicle_id\n" +
                " left join\n" +
                " (select tc.vin,'net_cat' car_type from vehicle_networking.t_net_car tc\n" +
                " union all select tt.vin,'taxi' car_type from vehicle_networking.t_taxi tt\n" +
                " union all select tp.vin,'private_car' car_type from vehicle_networking.t_private_car tp\n" +
                " union all select tm.vin,'model_car' car_type from vehicle_networking.t_model_car tm) t4\n" +
                " on t12.vin = t4.vin";
        //在指定的12个小时自动同步一次
        while(isRunning) {
            //执行SQL查询，返回数据集
            ResultSet rs = statement.executeQuery(sqlQuery);
            //定义一个 HashMap
            HashMap<String, VehicleInfoModel> map = new HashMap<String, VehicleInfoModel>();
            //遍历输出结果集
            while (rs.next()) {
                map.put(
                        rs.getString("vin"),
                        new VehicleInfoModel(
                                rs.getString("vin"),
                                rs.getString("model_code"),
                                rs.getString("model_name"),
                                rs.getString("series_code"),
                                rs.getString("series_name"),
                                rs.getString("product_date"),
                                rs.getString("car_type"),
                                rs.getString("nick_name"),
                                rs.getString((System.currentTimeMillis() -
                                        DateUtil.convertStringToDateTime(rs.getString("product_date"), DateFormatDefine.DATE_TIME_FORMAT).getTime()) / 1000 / 3600 / 24 / 365 + "")
                        )
                );
            }
            ctx.collect(map);
            //休眠12个小时
            TimeUnit.HOURS.sleep(12);
        }
    }

    /**
     * 实现取消自动读取数据
     */
    @Override
    public void cancel() {
        isRunning = false;
    }

    /**
     * 关闭连接和关闭 statement
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        if (!statement.isClosed()) {
            statement.close();
        }
        if (!conn.isClosed()) {
            conn.close();
        }
    }
}
