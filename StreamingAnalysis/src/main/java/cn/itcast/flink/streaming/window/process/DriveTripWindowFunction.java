package cn.itcast.flink.streaming.window.process;

import cn.itcast.flink.streaming.entity.ItcastDataObj;
import cn.itcast.flink.streaming.entity.TripModel;
import com.clearspring.analytics.util.Lists;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Author itcast
 * Date 2022/6/9 9:30
 * Desc 需求 - 统计15分钟行程中，用户在低速、中速、高速获取到的车辆驾驶行为数据
 * 1.soc剩余电量
 * 2.行驶里程
 * 3.行驶车速
 * 4.切换次数
 * 5.终端时间和vin等数据到 vehiclemodel 对象中
 */

/**
 * <IN> – The type of the input value.  ItcastDataObj
 * <OUT> – The type of the output value. TripModel
 * <KEY> – The type of the key. Vin : String
 * <W> – The type of Window that this window  TimeWindow
 */
public class DriveTripWindowFunction extends ProcessWindowFunction<ItcastDataObj, TripModel, String, TimeWindow> {
    /**
     * 将15分钟session窗口会话中的车辆数据最终封装为 TripModel
     *
     * @param vin
     * @param context
     * @param elements
     * @param out
     * @throws Exception
     */
    @Override
    public void process(String vin, ProcessWindowFunction<ItcastDataObj, TripModel, String, TimeWindow>.Context context, Iterable<ItcastDataObj> elements,
                        Collector<TripModel> out) throws Exception {
        //1.1 将迭代器转换成集合列表
        List<ItcastDataObj> itcastDataObjs = Lists.newArrayList(elements);
        //1.2 对集合列表的数据进行排序操作
        Collections.sort(itcastDataObjs, Comparator.comparing(ItcastDataObj::getTerminalTimeStamp));
        //1.3 将集合对象转换成 TripModel 对象返回
        TripModel tripModel = getTripModel(itcastDataObjs);
        //1.4 将 TripModel 对象收集返回
        out.collect(tripModel);
    }

    /**
     * 收集每个sessionwindows中数据记录将其封装到 TripModel 中
     * 要获取当前会话窗口内的第一条数据的信息（soc，行驶里程，行程时间，位置信息）
     * 要获取当前会话窗口内的最后一条数据的信息
     * 获取当前驾驶行程的窗口内的其他的数据（低速、中速、高速对应的电量soc，里程，经纬度等）
     * 将以上数据封装到 TripModel 中并返回
     *
     * @param itcastDataObjs
     * @return
     */
    private TripModel getTripModel(List<ItcastDataObj> itcastDataObjs) {
        TripModel tripModel = new TripModel();
        //2.1 获取第一条数据
        ItcastDataObj firstItcastDataObj = itcastDataObjs.get(0);
        //2.2 将 vin（车架号） ，tripStartTime（行程开始时间），start_BMS_SOC（行程开始Soc），start_longitude（行程开始经度），start_latitude（行程开始维度），start_mileage（行程开始表显里程数） 传递给 tripModel 对象。
        tripModel.setVin(firstItcastDataObj.getVin());
        tripModel.setTripStartTime(firstItcastDataObj.getTerminalTime());
        tripModel.setStart_BMS_SOC(firstItcastDataObj.getSoc());
        tripModel.setStart_longitude(firstItcastDataObj.getLng());
        tripModel.setStart_latitude(firstItcastDataObj.getLat());
        //odometer 表显，仪表盘显示的公里数
        tripModel.setStart_mileage(firstItcastDataObj.getTotalOdometer());
        //2.3 从最后一条数据中得到对象并赋值给 tripEndTime（行程结束时间），end_BMS_SOC（行程结束soc），end_longitude（行程结束经度），end_latitude（行程结束维度），end_mileage（行程结束表显里程数），mileage（行程驾驶公里数），time_comsuption（行程消耗时间）、这里存储的是分钟数，lastSoc（上次的行程Soc）、将当前行程开始的电量消耗百分比作为上一个行程结束的电量消耗百分比，lastMileage（上次的里程数）
        ItcastDataObj lastItcastDataObj = itcastDataObjs.get(itcastDataObjs.size() - 1);
        tripModel.setTripEndTime(lastItcastDataObj.getTerminalTime());
        tripModel.setEnd_BMS_SOC(lastItcastDataObj.getSoc());
        tripModel.setEnd_longitude(lastItcastDataObj.getLng());
        tripModel.setEnd_latitude(lastItcastDataObj.getLat());
        // 表显，行驶的总公里数
        tripModel.setEnd_mileage(lastItcastDataObj.getTotalOdometer());

        //2.4 遍历 itcastDataObj list获取一下内容
        for (ItcastDataObj itcastDataObj : itcastDataObjs) {
            // 每条数据的速度, km/h 车速
            Double currentSpeed = itcastDataObj.getSpeed();
            //获取上次行程报文的soc
            Double lastSoc = tripModel.getLastSoc();
            //计算每条数据的soc与lastSoc进行比较差值 socDiff
            double socDiff = itcastDataObj.getSoc() - lastSoc;
            //如果socDiff大于0，那么赋值给 soc_comsuption
            if (Math.abs(socDiff) > 0) {
                tripModel.setSoc_comsuption(socDiff);
            }
            //如果speed大于tripModel对象中保存的最大车速并且小于150，将speed保存为最大速度
            if (currentSpeed > tripModel.getMax_speed() && (currentSpeed < 150)) {
                tripModel.setMax_speed(currentSpeed);
            }
            //低速行驶 speed >=0 && <40,低速行驶个数+1；low_BMS_SOC=low_BMS_SOC+(当次油耗和上次油耗之差)；setLow_BMS_Mileage=setLow_BMS_Mileage+（当次里程和上次里程之差）
            if (currentSpeed > 0 && currentSpeed < 40) {
                //低速行驶
                tripModel.setLow_BMS_SOC(tripModel.getLow_BMS_SOC() + Math.abs(itcastDataObj.getSoc() - lastSoc));
                tripModel.setLow_BMS_Mileage(tripModel.getLow_BMS_Mileage() + Math.abs(itcastDataObj.getTotalOdometer() - tripModel.getLastMileage()));
                tripModel.setTotal_low_speed_nums(tripModel.getTotal_low_speed_nums() + 1);
            } else if (currentSpeed >= 40 && currentSpeed < 80) {
                tripModel.setMedium_BMS_SOC(tripModel.getMedium_BMS_SOC() + Math.abs(itcastDataObj.getSoc() - lastSoc));
                tripModel.setMedium_BMS_Mileage(tripModel.getMedium_BMS_Mileage() + Math.abs(itcastDataObj.getTotalOdometer() - tripModel.getLastMileage()));
                tripModel.setTotal_medium_speed_nums(tripModel.getTotal_medium_speed_nums() + 1);
            } else if (currentSpeed >= 80 && currentSpeed < 150) {
                tripModel.setHigh_BMS_SOC(tripModel.getHigh_BMS_SOC() + Math.abs(itcastDataObj.getSoc() - lastSoc));
                tripModel.setMedium_BMS_Mileage(tripModel.getHigh_BMS_Mileage() + Math.abs(itcastDataObj.getTotalOdometer() - tripModel.getLastMileage()));
                tripModel.setTotal_high_speed_nums(tripModel.getTotal_high_speed_nums() + 1);
            } else{
                System.out.println("当前超速异常！");
            }
            //中速行驶 >=40 && <80
            //高速行驶 >80 && <150
            //设置当前的soc作为 lastSoc 上次的soc 和将当次的里程作为 lastMileage 上次的里程
            tripModel.setLastSoc(itcastDataObj.getSoc() + 0D);
            tripModel.setLastMileage(itcastDataObj.getTotalOdometer() + 0D);
            //增加扩展字段，判断当前是否有异常数据，如果列表长度大于1说明是正常行程0，否则是异常行程1
            if (itcastDataObjs.size() >= 1) {
                tripModel.setTripStatus(0);
            }else{
                tripModel.setTripStatus(1);
            }
        }
        return tripModel;
    }
}
