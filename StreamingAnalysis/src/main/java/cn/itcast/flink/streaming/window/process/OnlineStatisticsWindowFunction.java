package cn.itcast.flink.streaming.window.process;

import cn.itcast.flink.streaming.entity.ItcastDataPartObj;
import cn.itcast.flink.streaming.entity.OnlineDataObj;
import cn.itcast.flink.streaming.util.DateFormatDefine;
import cn.itcast.flink.streaming.util.DateUtil;
import com.clearspring.analytics.util.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Author itcast
 * Date 2022/6/12 11:42
 * 此类中主要实现将告警数据封装到 OnlineDataObj 对象中
 * 告警数据 - 19项
 */

/**
 * IN:车辆数据
 * OUT: 车辆告警 + 车辆静态 + 车辆位置国家省市区 OnlineDataObj
 * String : vin
 * TimeWindow : 时间窗口对象
 */
public class OnlineStatisticsWindowFunction extends ProcessWindowFunction<ItcastDataPartObj, OnlineDataObj, String, TimeWindow> {

    //实现WindowFunction<ItcastDataPartObj, OnlineDataObj, String, TimeWindow>接口
    @Override
    public void process(String vin, ProcessWindowFunction<ItcastDataPartObj, OnlineDataObj, String, TimeWindow>.Context context, Iterable<ItcastDataPartObj> elements, Collector<OnlineDataObj> out) throws Exception {
        //1.对当前的数据集合进行升序排列
        List<ItcastDataPartObj> itcastDataPartObjs = Lists.newArrayList(elements);
        Collections.sort(itcastDataPartObjs, Comparator.comparing(ItcastDataPartObj::getTerminalTimeStamp));
        //2.获取集合中第一条数据
        ItcastDataPartObj firstItcastDataObj = itcastDataPartObjs.get(0);
        //3.循环遍历每条数据，将集合中存在异常的数据拼接到指定属性中
        for (ItcastDataPartObj itcastDataPartObj : itcastDataPartObjs) {
            OnlineDataObj onlineDataObj = null;
            //30s窗口最多6条数据，每条数据需要检测19个字段，如果出现异常字段就进行  //字符串拼接
            //3.1.过滤没有各种告警的信息，调用setOnlineDataObj 将第一条对象和每条对象和标识0 返回到OnlineDataObj，并收集这个对象
            // 否则 调用setOnlineDataObj 将第一条对象和每条对象和标识1 返回到OnlineDataObj，并收集这个对象
            if(filterNoAlarm(itcastDataPartObj)){
                onlineDataObj = setOnlineDataObj(firstItcastDataObj,itcastDataPartObj,0);
            }else{
                onlineDataObj = setOnlineDataObj(firstItcastDataObj,itcastDataPartObj,1);
            }
            //收集数据
            out.collect(onlineDataObj);
        }
    }

    /**
     * 封装 OnlineDataObj 对象
     * @param firstItcastDataObj
     * @param itcastDataPartObj
     * @param flag
     * @return
     */
    private OnlineDataObj setOnlineDataObj(ItcastDataPartObj firstItcastDataObj, ItcastDataPartObj itcastDataPartObj, int flag) throws InvocationTargetException, IllegalAccessException {
        //4.1.定义OnlineDataObj
        OnlineDataObj onlineDataObj = new OnlineDataObj();
        //4.2.将每条的对象属性拷贝到定义OnlineDataObj
        BeanUtils.copyProperties(onlineDataObj,itcastDataPartObj);
        //4.3.将每条对象中表显里程赋值给mileage
        onlineDataObj.setMileage(itcastDataPartObj.getTotalOdometer());
        //4.4.将告警信号赋值给isAlarm
        onlineDataObj.setIsAlarm(flag);
        //4.5.将每个对象通过addAlarmNameList生成告警list，拼接成字符串赋值给alarmName，通过字符串join
        String alarmName = String.join("~", addAlarmNameList(itcastDataPartObj));
        onlineDataObj.setAlarmName(alarmName);
        //4.6.将窗口内第一条数据告警时间赋值给 earliestTime
        onlineDataObj.setEarliestTime(firstItcastDataObj.getTerminalTime());
        //4.7.将获取每条记录的充电状态通过getChargeState返回充电标识赋值给充电标记 ,充电的为 0 ，不充电为 1
        onlineDataObj.setChargeFlag(getChargeState(itcastDataPartObj.getChargeStatus()));
        //4.8.将当前时间赋值给处理时间
        onlineDataObj.setProcessTime(DateUtil.getCurrentDateTime(DateFormatDefine.DATE_TIME_FORMAT));
        //引入-判断是否存在报警的字段，addAlarmNameList，getChargeState
        return onlineDataObj;
    }

    /**
     * 根据充电状态返回充电标记
     * @param chargeState
     * @return
     */
    private Integer getChargeState(Integer chargeState){
        int chargeFlag = -999999;//充电状态的初始值
        //充电状态：0x01: 停车充电、 0x02: 行车充电
        if(chargeState == 1 || chargeState==2){
            chargeFlag = 1;
        }
        //0x04:充电完成 0x03: 未充电
        else if(chargeState == 4 || chargeState == 3){
            chargeFlag = 0;
        }else{
            //否则属于异常
            chargeFlag = 2;
        }
        return chargeFlag;
    }

    /**
     * 将每条数据的故障名称追加到故障名称列表中
     * @return
     */
    private ArrayList<String> addAlarmNameList(ItcastDataPartObj dataPartObj) {
        //定义故障名称列表对象
        ArrayList<String> alarmNameList = new ArrayList<>();
        //电池高温报警
        if (dataPartObj.getBatteryAlarm() == 1) {
            alarmNameList.add("电池高温报警");
        }
        //单体电池高压报警
        if (dataPartObj.getSingleBatteryOverVoltageAlarm() == 1) {
            alarmNameList.add("单体电池高压报警");
        }
        //电池单体一致性差报警
        if (dataPartObj.getBatteryConsistencyDifferenceAlarm() == 1) {
            alarmNameList.add("电池单体一致性差报警");
        }
        //绝缘报警
        if (dataPartObj.getInsulationAlarm() == 1) {
            alarmNameList.add("绝缘报警");
        }
        //高压互锁状态报警
        if (dataPartObj.getHighVoltageInterlockStateAlarm() == 1) {
            alarmNameList.add("高压互锁状态报警");
        }
        //SOC跳变报警
        if (dataPartObj.getSocJumpAlarm() == 1) {
            alarmNameList.add("SOC跳变报警");
        }
        //驱动电机控制器温度报警
        if (dataPartObj.getDriveMotorControllerTemperatureAlarm() == 1) {
            alarmNameList.add("驱动电机控制器温度报警");
        }
        //DC-DC温度报警（dc-dc可以理解为车辆动力智能系统转换器）
        if (dataPartObj.getDcdcTemperatureAlarm() == 1) {
            alarmNameList.add("DC-DC温度报警");
        }
        //SOC过高报警
        if (dataPartObj.getSocHighAlarm() == 1) {
            alarmNameList.add("SOC过高报警");
        }
        //SOC低报警
        if (dataPartObj.getSocLowAlarm() == 1) {
            alarmNameList.add("SOC低报警");
        }
        //温度差异报警
        if (dataPartObj.getTemperatureDifferenceAlarm() == 1) {
            alarmNameList.add("温度差异报警");
        }
        //车载储能装置欠压报警
        if (dataPartObj.getVehicleStorageDeviceUndervoltageAlarm() == 1) {
            alarmNameList.add("车载储能装置欠压报警");
        }
        //DC-DC状态报警
        if (dataPartObj.getDcdcStatusAlarm() == 1) {
            alarmNameList.add("DC-DC状态报警");
        }
        //单体电池欠压报警
        if (dataPartObj.getSingleBatteryUnderVoltageAlarm() == 1) {
            alarmNameList.add("单体电池欠压报警");
        }
        //可充电储能系统不匹配报警
        if (dataPartObj.getRechargeableStorageDeviceMismatchAlarm() == 1) {
            alarmNameList.add("可充电储能系统不匹配报警");
        }
        //车载储能装置过压报警
        if (dataPartObj.getVehicleStorageDeviceOvervoltageAlarm() == 1) {
            alarmNameList.add("车载储能装置过压报警");
        }
        //制动系统报警
        if (dataPartObj.getBrakeSystemAlarm() == 1) {
            alarmNameList.add("制动系统报警");
        }
        //驱动电机温度报警
        if (dataPartObj.getDriveMotorTemperatureAlarm() == 1) {
            alarmNameList.add("驱动电机温度报警");
        }
        //车载储能装置类型过充报警
        if (dataPartObj.getVehiclePureDeviceTypeOvercharge() == 1) {
            alarmNameList.add("车载储能装置类型过充报警");
        }
        return alarmNameList;
    }

    /**
     * 判断当前这条数据是否包含19项告警中的任意一个告警，如果包含就为 true ，否则为 false
     * 只要有一个告警，就为 false ，否则为 true
     * @param dataPartObj
     * @return
     */
    private boolean filterNoAlarm(ItcastDataPartObj dataPartObj) {
        //电池高温报警
        if ((dataPartObj.getBatteryAlarm() == 1) ||
                //单体电池高压报警
                dataPartObj.getSingleBatteryOverVoltageAlarm() == 1 ||
                //电池单体一致性差报警
                dataPartObj.getBatteryConsistencyDifferenceAlarm() == 1 ||
                //绝缘报警
                dataPartObj.getInsulationAlarm() == 1 ||
                //高压互锁状态报警
                dataPartObj.getHighVoltageInterlockStateAlarm() == 1 ||
                //SOC跳变报警
                dataPartObj.getSocJumpAlarm() == 1 ||
                //驱动电机控制器温度报警
                dataPartObj.getDriveMotorControllerTemperatureAlarm() == 1 ||
                //DC-DC温度报警（dc-dc可以理解为车辆动力智能系统转换器）
                dataPartObj.getDcdcTemperatureAlarm() == 1 ||
                //SOC过高报警
                dataPartObj.getSocHighAlarm() == 1 ||
                //SOC低报警
                dataPartObj.getSocLowAlarm() == 1 ||
                //温度差异报警
                dataPartObj.getTemperatureDifferenceAlarm() == 1 ||
                //车载储能装置欠压报警
                dataPartObj.getVehicleStorageDeviceUndervoltageAlarm() == 1 ||
                //DC-DC状态报警
                dataPartObj.getDcdcStatusAlarm() == 1 ||
                //单体电池欠压报警
                dataPartObj.getSingleBatteryUnderVoltageAlarm() == 1 ||
                //可充电储能系统不匹配报警
                dataPartObj.getRechargeableStorageDeviceMismatchAlarm() == 1 ||
                //车载储能装置过压报警
                dataPartObj.getVehicleStorageDeviceOvervoltageAlarm() == 1 ||
                //制动系统报警
                dataPartObj.getBrakeSystemAlarm() == 1 ||
                //驱动电机温度报警
                dataPartObj.getDriveMotorTemperatureAlarm() == 1 ||
                //车载储能装置类型过充报警
                dataPartObj.getVehiclePureDeviceTypeOvercharge() == 1
        ) {
            return false;
        } else {
            return true;
        }
    }
}
