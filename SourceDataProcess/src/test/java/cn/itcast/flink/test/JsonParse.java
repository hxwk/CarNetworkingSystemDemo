package cn.itcast.flink.test;

import cn.itcast.flink.entity.CarModel;
import org.json.JSONObject;

/**
 * Author itcast
 * Date 2022/6/4 11:01
 * Desc 需求 - 解析json字符串
 * 开发步骤：
 *  1.定义 json 字符串
 *  2.封装对象，将json解析出来的数据保存到对象中
 *  3.打印输出并查验结果
 */
public class JsonParse {
    public static void main(String[] args) {
        //1.定义 json 字符串
        String json = "{\"batteryAlarm\": 0, \"carMode\": 1,\"minVoltageBattery\": 3.89, \"chargeStatus\": 1,\"vin\":\"LS5A3CJC0JF890971\"}";
        //2.封装对象，将json解析出来的数据保存到对象中
        JSONObject jsonObject = new JSONObject(json);
        int batteryAlarm = jsonObject.getInt("batteryAlarm");
        int carMode = jsonObject.getInt("carMode");
        double minVoltageBattery = jsonObject.getDouble("minVoltageBattery");
        int chargeStatus = jsonObject.getInt("chargeStatus");
        String vin = jsonObject.getString("vin");

        CarModel carModel = new CarModel(batteryAlarm, carMode, minVoltageBattery, chargeStatus, vin);
        //3.打印输出并查验结果
        System.out.println(carModel.toString());
    }
}
