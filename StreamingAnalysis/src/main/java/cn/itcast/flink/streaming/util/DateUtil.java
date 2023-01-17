package cn.itcast.flink.streaming.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Author itcast
 * Date 2022/6/4 16:15
 * Desc 日期工具类，提供两个方法
 * 1.获取当前的时间
 * 2.指定一个时间返回一个时间戳
 */
public class DateUtil {
    /**
     * 根据指定的格式来获取当前时间
     * @param formatDefine
     * @return
     */
    // todo 定义方法 getCurrentDateTime（DateFormatDefine formatDefine） 获取当前时间字符串，根据指定的DateFormat
    public static String getCurrentDateTime(DateFormatDefine formatDefine) {
        //1.初始化 DateFormat
        SimpleDateFormat sdf = new SimpleDateFormat(formatDefine.getFormat());
        //2.获取当前时间
        long currentTimestamp = System.currentTimeMillis();
        //3.根据格式类型转换成字符串
        String currentDate = sdf.format(currentTimestamp);
        //4.返回这个时间字符串
        return currentDate;
    }

    /**
     * 将时间字符串根据时间字符串类型转换成 Date 类型
     * @param dateStr
     * @param formatDefine
     * @return
     */
    // todo 定义方法 convertStringToDateTime（String dateStr, DateFormatDefine formatDefine）通过Date字符串和DateFormat转换成 Date
    public static Date convertStringToDateTime(String dateStr, DateFormatDefine formatDefine){
        //拿到了传递过来的值的参数
        SimpleDateFormat sdf = new SimpleDateFormat(formatDefine.getFormat());
        //将当前的时间字符串转换成了时间类型
        Date date = null;
        try {
            date = sdf.parse(dateStr);
        } catch (ParseException e) {
            System.out.println("当前时间格式化异常！");
            e.printStackTrace();
        }
        return date;
    }
}
