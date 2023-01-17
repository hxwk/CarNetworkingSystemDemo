package cn.itcast.flink.test;

import cn.itcast.flink.entity.CarModelPlus;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * Author itcast
 * Date 2022/6/4 11:34
 * Desc 需求 - 将复杂的json（有序集合）解析成对象
 * 将 jsonobject 字符串统一生成 Map 对象
 */
public class JsonPlusParseOptimize {
    public static void main(String[] args) {
        //1.定义json字符串
        String json = "{\"batteryAlarm\": 0,\"carMode\": 1,\"minVoltageBattery\": 3.89,\"chargeStatus\": 1,\"vin\": \"LS5A3CJC0JF890971\",\"nevChargeSystemTemperatureDtoList\": [{\"probeTemperatures\": [25, 23, 24, 21, 24, 21, 23, 21, 23, 21, 24, 21, 24, 21, 25, 21],\"chargeTemperatureProbeNum\": 16,\"childSystemNum\": 1}]}";
        //将 json 字符串解析成 Map 对象，便于后续的 value = get(key)
        Map<String, Object> hashMap = toMap(json);
        //3.将json key对应的值获取出来并封装到对象中
        int batteryAlarm = convertInt(hashMap, "batteryAlarm");
        int carMode = convertInt(hashMap, "carMode");
        double minVoltageBattery = convertDouble(hashMap, "minVoltageBattery");
        int chargeStatus = convertInt(hashMap, "chargeStatus");
        String vin = convert(hashMap, "vin");

        //根据有序集合的key=nevChargeSystemTemperatureDtoList 获取数组的信息
        JSONArray nevChargeSystemTempArr = new JSONArray(hashMap.get("nevChargeSystemTemperatureDtoList").toString());
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

    private static double convertDouble(Map<String, Object> hashMap, String key) {
        Object value = hashMap.get(key);
        //将value转换成 double
        if (value != null) {
            double v = Double.parseDouble(value.toString());
            return v;
        }else{
            return -999999;
        }
    }

    private static int convertInt(Map<String, Object> hashMap, String key) {
        Object value = hashMap.get(key);
        //将value转换成 double
        if (value != null) {
            int v = Integer.parseInt(value.toString());
            return v;
        }else{
            return -999999;
        }
    }

    private static String convert(Map<String, Object> hashMap, String key) {
        Object value = hashMap.get(key);
        //将value转换成 double
        if (value != null) {
            String v = value.toString();
            return v;
        }else{
            return -999999 + "";
        }
    }

    /**
     * 将 json 字符串转换成 Map 对象
     *
     * @param json
     * @return
     */
    private static Map<String, Object> toMap(String json) {
        //定义待返回的 Map 对象
        Map<String, Object> entry = new HashMap<>();
        //将json字符串转换成 json对象
        JSONObject jsonObject = new JSONObject(json);
        //获取所有的 json对象的 keys
        Iterator<String> keys = jsonObject.keys();
        //遍历这些key，获取每个key对应的值
        while (keys.hasNext()) {
            String key = keys.next();
            //获取 value 的值
            Object object = jsonObject.get(key);
            //封装到对象中
            entry.put(key, object);
        }
        return entry;
    }
}
