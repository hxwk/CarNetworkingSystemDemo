package cn.itcast.flink.test;

import cn.itcast.flink.entity.CarModelPlus;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Author itcast
 * Date 2022/6/4 11:34
 * Desc 需求 - 将复杂的json（有序集合）解析成对象
 */
public class JsonPlusParse {
    public static void main(String[] args) {
        //1.定义json字符串
        String json = "{\"batteryAlarm\": 0,\"carMode\": 1,\"minVoltageBattery\": 3.89,\"chargeStatus\": 1,\"vin\": \"LS5A3CJC0JF890971\",\"nevChargeSystemTemperatureDtoList\": [{\"probeTemperatures\": [25, 23, 24, 21, 24, 21, 23, 21, 23, 21, 24, 21, 24, 21, 25, 21],\"chargeTemperatureProbeNum\": 16,\"childSystemNum\": 1}]}";
        //2.将json字符串转换成对象JSONObject
        JSONObject jsonObject = new JSONObject(json);
        //3.将json key对应的值获取出来并封装到对象中
        int batteryAlarm = jsonObject.getInt("batteryAlarm");
        int carMode = jsonObject.getInt("carMode");
        double minVoltageBattery = jsonObject.getDouble("minVoltageBattery");
        int chargeStatus = jsonObject.getInt("chargeStatus");
        String vin = jsonObject.getString("vin");

        //根据有序集合的key=nevChargeSystemTemperatureDtoList 获取数组的信息
        JSONArray nevChargeSystemTempArr = jsonObject.getJSONArray("nevChargeSystemTemperatureDtoList");
        //取出第一个位置上的json对象
        JSONObject chargeSystemObject = nevChargeSystemTempArr.getJSONObject(0);
        int chargeTemperatureProbeNum = chargeSystemObject.getInt("chargeTemperatureProbeNum");
        int childSystemNum = chargeSystemObject.getInt("childSystemNum");
        //将探针的每个温度值都取出来并放到一个集合中
        List<Integer> probeTemperatures = new ArrayList<>();
        JSONArray probeTemperaturesArray = chargeSystemObject.getJSONArray("probeTemperatures");
        for (Object probeTemperature : probeTemperaturesArray) {
            probeTemperatures.add(Integer.parseInt(probeTemperature.toString()));
        }

        //封装这个对象
        CarModelPlus carModel = new CarModelPlus(
                batteryAlarm,
                carMode,
                minVoltageBattery,
                chargeStatus,
                vin,
                probeTemperatures,
                chargeTemperatureProbeNum,
                childSystemNum
        );
        //4.打印输出对象
        System.out.println(carModel.toString());
        //5.查验结果
    }
}
