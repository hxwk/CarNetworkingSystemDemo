package cn.itcast.flink.streaming.process;

import cn.itcast.flink.streaming.entity.ElectricFenceModel;
import cn.itcast.flink.streaming.entity.ElectricFenceResultTmp;
import cn.itcast.flink.streaming.entity.ItcastDataPartObj;
import cn.itcast.flink.streaming.util.DistanceCaculateUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.jasper.util.FastDateFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;

/**
 * Author itcast
 * Date 2022/6/9 17:09
 * 该类主要实现，将实时上报的车辆数据和电子围栏数据进行合并操作封装成类 ElectricFenceModel 对象
 */
public class ElectricFenceRulesFuntion extends BroadcastProcessFunction<ItcastDataPartObj,
        HashMap<String, ElectricFenceResultTmp>,
        ElectricFenceModel> {
    //定义一个输出的日志 Logger
    final static Logger logger = LoggerFactory.getLogger(ElectricFenceRulesFuntion.class);

    //定义广播变量中的类型
    MapStateDescriptor<String, ElectricFenceResultTmp> electricFenceStateDesc =
            new MapStateDescriptor<>("electricFence", Types.STRING, Types.POJO(ElectricFenceResultTmp.class));

    //实现处理每条数据
    @Override
    public void processElement(ItcastDataPartObj vehicle, BroadcastProcessFunction<ItcastDataPartObj,
            HashMap<String, ElectricFenceResultTmp>, ElectricFenceModel>.ReadOnlyContext ctx,
                               Collector<ElectricFenceModel> out) throws Exception {
        //定义返回的对象
        ElectricFenceModel model = new ElectricFenceModel();
        //2.判断如果流数据数据质量（车辆的经纬度不能为0或-999999，车辆GpsTime不能为空）
        if (vehicle.getLng() != 0 && vehicle.getLng() != -999999 && vehicle.getGpsTime() != null) {
            //2.1.获取当前车辆的 vin
            String vin = vehicle.getVin();
            //2.2.通过vin获取电子围栏的配置信息
            //先获取电子围栏的数据，在广播中
            ElectricFenceResultTmp electricFenceResultTmp = ctx.getBroadcastState(electricFenceStateDesc).get(vin);
            //2.3.如果电子围栏配置信息不为空
            if (electricFenceResultTmp != null) {
                //定义时间格式化
                FastDateFormat df = new FastDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

                //2.3.1.说明当前车辆关联了电子围栏规则，需要判断当前上报的数据是否在电子围栏规则的生效时间内，
                // 先获取上报地理位置时间gpsTimestamp
                //2.3.2.如果当前gpsTimestamp>=开始时间戳并且gpsTimestamp<=结束时间戳，以下内容存入到 ElectricFenceModel
                if (df.parse(vehicle.getGpsTime()).getTime() >= electricFenceResultTmp.getStartTime().getTime()
                        && df.parse(vehicle.getGpsTime()).getTime() <= electricFenceResultTmp.getEndTime().getTime()) {
                    //2.3.2.1.上报车辆的数据在电子围栏生效期内 vin gpstime lng lat 终端时间和终端时间戳
                    model.setVin(vin);
                    model.setGpsTime(vehicle.getGpsTime());
                    model.setLng(vehicle.getLng());
                    model.setLat(vehicle.getLat());
                    model.setTerminalTime(vehicle.getTerminalTime());
                    model.setTerminalTimestamp(vehicle.getTerminalTimeStamp());
                    //2.3.2.2.电子围栏id，电子围栏名称，地址，半径
                    model.setEleId(electricFenceResultTmp.getId());
                    model.setEleName(electricFenceResultTmp.getName());
                    model.setRadius(electricFenceResultTmp.getRadius());
                    model.setAddress(electricFenceResultTmp.getAddress());
                    //2.3.2.3.电子围栏经纬度
                    model.setLongitude(electricFenceResultTmp.getLongitude());
                    model.setLatitude(electricFenceResultTmp.getLatitude());
                    //2.3.2.4.计算经纬度和电子围栏经纬度距离距离，如果两点之间大于半径（单位是千米）的距离，就是存在于圆外，否则反之
                    Double distance = DistanceCaculateUtil
                            .getDistance(vehicle.getLat(), vehicle.getLng(), electricFenceResultTmp.getLatitude(),
                                    electricFenceResultTmp.getLongitude());
                    //判断两个中心点和当前车辆的坐标球面距离是否大于半径，大于在圈外，否则在圈内
                    if (distance > electricFenceResultTmp.getRadius()) {
                        model.setNowStatus(1);
                    } else {
                        model.setNowStatus(0);
                    }
                    //2.3.2.5.收集结果数据
                    out.collect(model);
                } else {
                    logger.warn("当前gps时间不在电子围栏的开始和结束有效时间内，请检查！");
                }

            } else {
                logger.warn("当前vin对应电子围栏的数据为空，请检查");
            }
        } else {
            logger.warn("当前的车辆 gpstime 不能为空或经纬度异常，请检查");
        }
    }

    //处理广播数据
    @Override
    public void processBroadcastElement(HashMap<String, ElectricFenceResultTmp> value,
                                        BroadcastProcessFunction<ItcastDataPartObj, HashMap<String, ElectricFenceResultTmp>,
                                                ElectricFenceModel>.Context ctx, Collector<ElectricFenceModel> out) throws Exception {
        BroadcastState<String, ElectricFenceResultTmp> broadcastState = ctx.getBroadcastState(electricFenceStateDesc);
        //清空这个广播变量
        broadcastState.clear();
        //加载新的广播变量
        broadcastState.putAll(value);
    }
}
