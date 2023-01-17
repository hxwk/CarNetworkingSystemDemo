package cn.itcast.flink.streaming.sink;

import cn.itcast.flink.streaming.entity.OnlineDataObj;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * Author itcast
 * Date 2022/6/12 15:53
 * 将分析的在线告警统计分析结果落地到 mysql 数据表中
 */
public class OnlineStatisticsMysqlSink extends RichSinkFunction<OnlineDataObj> {
    private static Logger logger = LoggerFactory.getLogger(OnlineStatisticsMysqlSink.class);

    //定义connection连接对象
    Connection connection = null;
    //定义statement
    PreparedStatement pstmt = null;
    //定义boolean, 是否运行的标记
    Boolean isRunning = true;

    /**
     * 初始化操作。初始化mysql的连接对象
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
        connection = DriverManager.getConnection(url, username, password);
        //设置当前不自动提交
        connection.setAutoCommit(false);
        //实例化statement
        String sql = "INSERT INTO online_data(vin,process_time,lat,lng, mileage,is_alarm,alarm_name,terminal_time,earliest_time,max_voltage_battery,min_voltage_battery,max_temperature_value,min_temperature_value,speed,soc,charge_flag,total_voltage,total_current,battery_voltage,probe_temperatures,series_name,model_name,live_time,sales_date,car_type,province,city, county) values(?,now(),?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,?,?) \n" +
                "ON DUPLICATE KEY UPDATE process_time=now(),lat=?,lng=?,mileage=?,is_alarm=?, alarm_name=?,terminal_time=?,max_voltage_battery=?,\n" +
                "min_voltage_battery=?,max_temperature_value=?, min_temperature_value=?, speed=?,soc=?,charge_flag=?, total_voltage=?,\n" +
                "total_current=?,battery_voltage=?, probe_temperatures=?,series_name=?,model_name=?,live_time=?,sales_date=?,car_type=?,\n" +
                "province=?,city=?,county=?";
        //String sql = "INSERT INTO online_data(vin,process_time,lat,lng, mileage,is_alarm,alarm_name,terminal_time,earliest_time,max_voltage_battery,min_voltage_battery,max_temperature_value,min_temperature_value,speed,soc,charge_flag,total_voltage,total_current,battery_voltage,probe_temperatures,series_name,model_name,live_time,sales_date,car_type,province,city, county) values(?,now(),?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,?,?)";
        pstmt = connection.prepareStatement(sql);
    }

    /**
     * 关闭释放资源
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (pstmt != null) pstmt.close();
        if (connection != null) connection.close();
    }

    /**
     * @param onlineDataObj
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(OnlineDataObj onlineDataObj, Context context) throws Exception {
        try {
            //插入数据的参数
            pstmt.setString(1, onlineDataObj.getVin());
            pstmt.setDouble(2, onlineDataObj.getLat());
            pstmt.setDouble(3, onlineDataObj.getLng());
            pstmt.setDouble(4, onlineDataObj.getMileage());
            pstmt.setInt(5, onlineDataObj.getIsAlarm());
            pstmt.setString(6, onlineDataObj.getAlarmName());
            pstmt.setString(7, onlineDataObj.getTerminalTime());
            pstmt.setString(8, onlineDataObj.getEarliestTime());
            pstmt.setDouble(9, onlineDataObj.getMaxVoltageBattery());
            pstmt.setDouble(10, onlineDataObj.getMinVoltageBattery());
            pstmt.setDouble(11, onlineDataObj.getMaxTemperatureValue());
            pstmt.setDouble(12, onlineDataObj.getMinTemperatureValue());
            pstmt.setDouble(13, onlineDataObj.getSpeed());
            pstmt.setInt(14, onlineDataObj.getSoc());
            pstmt.setInt(15, onlineDataObj.getChargeFlag());
            pstmt.setDouble(16, onlineDataObj.getTotalVoltage());
            pstmt.setDouble(17, onlineDataObj.getTotalCurrent());
            pstmt.setString(18, onlineDataObj.getBatteryVoltage());
            pstmt.setString(19, onlineDataObj.getProbeTemperatures());
            pstmt.setString(20, onlineDataObj.getSeriesName());
            pstmt.setString(21, onlineDataObj.getModelName());
            pstmt.setString(22, onlineDataObj.getLiveTime());
            pstmt.setString(23, onlineDataObj.getSalesDate());
            pstmt.setString(24, onlineDataObj.getCarType());
            pstmt.setString(25, onlineDataObj.getProvince());
            pstmt.setString(26, onlineDataObj.getCity());
            pstmt.setString(27, onlineDataObj.getCounty());
            //修改数据的参数
            pstmt.setDouble(28, onlineDataObj.getLat());
            pstmt.setDouble(29, onlineDataObj.getLng());
            pstmt.setDouble(30, onlineDataObj.getMileage());
            pstmt.setInt(31, onlineDataObj.getIsAlarm());
            pstmt.setString(32, onlineDataObj.getAlarmName());
            pstmt.setString(33, onlineDataObj.getTerminalTime());
            pstmt.setDouble(34, onlineDataObj.getMaxVoltageBattery());
            pstmt.setDouble(35, onlineDataObj.getMinVoltageBattery());
            pstmt.setDouble(36, onlineDataObj.getMaxTemperatureValue());
            pstmt.setDouble(37, onlineDataObj.getMinTemperatureValue());
            pstmt.setDouble(38, onlineDataObj.getSpeed());
            pstmt.setInt(39, onlineDataObj.getSoc());
            pstmt.setInt(40, onlineDataObj.getChargeFlag());
            pstmt.setDouble(41, onlineDataObj.getTotalVoltage());
            pstmt.setDouble(42, onlineDataObj.getTotalCurrent());
            pstmt.setString(43, onlineDataObj.getBatteryVoltage());
            pstmt.setString(44, onlineDataObj.getProbeTemperatures());
            pstmt.setString(45, onlineDataObj.getSeriesName());
            pstmt.setString(46, onlineDataObj.getModelName());
            pstmt.setString(47, onlineDataObj.getLiveTime());
            pstmt.setString(48, onlineDataObj.getSalesDate());
            pstmt.setString(49, onlineDataObj.getCarType());
            pstmt.setString(50, onlineDataObj.getProvince());
            pstmt.setString(51, onlineDataObj.getCity());
            pstmt.setString(52, onlineDataObj.getCounty());

            //执行数据更新和递交操作
            pstmt.execute();
            connection.commit();
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            connection.rollback();
        } finally {
            if (!pstmt.isClosed()) {
                pstmt.close();
            }
            if (!connection.isClosed()) {
                connection.close();
            }
        }
    }
}
