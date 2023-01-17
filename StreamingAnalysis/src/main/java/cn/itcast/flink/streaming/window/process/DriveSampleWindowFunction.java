package cn.itcast.flink.streaming.window.process;

import cn.itcast.flink.streaming.entity.ItcastDataObj;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 * Author itcast
 * Date 2022/6/8 16:44
 * Desc 实现驾驶行程采样分析的核心逻辑
 * 计算出每5s 每10s.. 车辆的驾驶行程数据（soc电量百分比、行驶里程、行驶车速等）ItcastDataObj
 */

/**
 * T:ItcastDataObj  R:String[] K: VIN  W:TimeWindow
 * 需求 - process 计算会话窗口（15m）内的数据，将窗口内的数据进行排序操作
 * 每 5s 将数据（soc剩余电量、车速、经纬度、终端时间等）保存起来，封装到数组汇总，最终返回这个数组
 */
public class DriveSampleWindowFunction extends ProcessWindowFunction<ItcastDataObj, String[], String, TimeWindow> {
    //创建驾驶行程采集数据自定义函数开发类 窗口内数据按照5s，10s，20s维度进行数据的收集和分析，此类继承于RichWindowFunction 抽象类
    //1.重写 process 方法
    /**
     * @param vin
     * @param context  当前上下文
     * @param elements 存储当前会话窗口内的所有事件
     * @param out      输出收集器
     * @throws Exception
     */
    @Override
    public void process(String vin,
                        ProcessWindowFunction<ItcastDataObj, String[], String, TimeWindow>.Context context, Iterable<ItcastDataObj> elements,
                        Collector<String[]> out) throws Exception {
        //1.1 将迭代器转换成集合列表
        ArrayList<ItcastDataObj> itcastDataObjs = Lists.newArrayList(elements);
        //1.2 对集合列表的数据进行排序操作
        Collections.sort(itcastDataObjs, new Comparator<ItcastDataObj>() {
            @Override
            public int compare(ItcastDataObj o1, ItcastDataObj o2) {
                if (o1.getTerminalTimeStamp() > o2.getTerminalTimeStamp()) {
                    return 1;
                } else if (o1.getTerminalTimeStamp() < o2.getTerminalTimeStamp()) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });
        //1.3 首先要获取排序后的第一条数据从而得到周期（5S等）的开始时间
        ItcastDataObj firstItcastDataObj = itcastDataObjs.get(0);
        //1.4 采样数据的soc(剩余电量百分比)、mileage（累计里程）、speed（车速）、gps（经度+维度）、terminalTime（终端时间）字段属性需要拼接到一个字符串返回
        //soc(剩余电量百分比) 缓存
        StringBuffer socBuffer = new StringBuffer(firstItcastDataObj.getSoc());
        //mileage（累计里程）
        StringBuffer mileageBuffer = new StringBuffer(firstItcastDataObj.getTotalOdometer() + "");
        //speed（车速）
        StringBuffer speedBuffer = new StringBuffer(firstItcastDataObj.getSpeed() + "");
        //gps：地理位置
        StringBuffer gpsBuffer = new StringBuffer(firstItcastDataObj.getLng() + "|" + firstItcastDataObj.getLat());
        //terminalTime：终端时间
        StringBuffer terminalTime = new StringBuffer(firstItcastDataObj.getTerminalTime());
        //1.5 获取排序后的最后一条数据作为当前窗口的最后一条数据
        ItcastDataObj lastItcastDataObj = itcastDataObjs.get(itcastDataObjs.size() - 1);
        //1.6 获取窗口的第一条数据的终端时间作为开始时间戳
        Long startTime = firstItcastDataObj.getTerminalTimeStamp();
        //1.7 获取窗口的最后一条数据的终端时间作为结束时间戳
        Long endTime = lastItcastDataObj.getTerminalTimeStamp();
        //1.8 遍历窗口内的每条数据，计算5S采样周期内的数据
        for (ItcastDataObj itcastDataObj : itcastDataObjs) {
            Long currentTime = itcastDataObj.getTerminalTimeStamp();
            if ((currentTime - startTime) / 1000 > 5 || (startTime == endTime)) {
                socBuffer.append(",").append(itcastDataObj.getSoc());
                mileageBuffer.append(",").append(itcastDataObj.getTotalOdometer());
                speedBuffer.append(",").append(itcastDataObj.getSpeed());
                gpsBuffer.append(",").append(itcastDataObj.getLng() + "|" + itcastDataObj.getLat());
                terminalTime.append(",").append(itcastDataObj.getTerminalTime());
                //将当前的时间指向开始时间
                startTime = currentTime;
            }
        }
        //1.9 创建字符串数组类型用于存储采集到的车辆唯一编码，终端时间戳，剩余电量，总里程数，车速，地理gps，终端时间
        String[] datas = new String[7];
        datas[0] = vin;
        datas[1] = firstItcastDataObj.getTerminalTimeStamp() + "";
        datas[2] = socBuffer.toString();
        datas[3] = mileageBuffer.toString();
        datas[4] = speedBuffer.toString();
        datas[5] = gpsBuffer.toString();
        datas[6] = terminalTime.toString();

        //1.10 返回数据
        out.collect(datas);
    }
}
