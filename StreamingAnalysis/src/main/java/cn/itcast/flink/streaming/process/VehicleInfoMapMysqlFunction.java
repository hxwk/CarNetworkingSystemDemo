package cn.itcast.flink.streaming.process;

import cn.itcast.flink.streaming.entity.OnlineDataObj;
import cn.itcast.flink.streaming.entity.VehicleInfoModel;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Author itcast
 * Date 2022/6/12 15:27
 * 此类主要用于将两个数据流进行合并，分别是车辆动态数据（位置数据+告警数据）和车辆静态数据（车型、车系、liveTime）
 */
public class VehicleInfoMapMysqlFunction extends BroadcastProcessFunction<OnlineDataObj, HashMap<String, VehicleInfoModel>, OnlineDataObj> {

    MapStateDescriptor<String, VehicleInfoModel> vehicleInfoModelDesc = new MapStateDescriptor<>(
            "vehicleInfoModel",
            Types.STRING,
            Types.POJO(VehicleInfoModel.class)
    );

    /**
     * 处理每条实时上报的车辆数据
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(OnlineDataObj value,
                               BroadcastProcessFunction<OnlineDataObj, HashMap<String, VehicleInfoModel>, OnlineDataObj>.ReadOnlyContext ctx,
                               Collector<OnlineDataObj> out) throws Exception {
        //1.1.通过 vin 获取到车辆静态信息 mysql
        VehicleInfoModel vehicleInfoModel = ctx.getBroadcastState(vehicleInfoModelDesc).get(value.getVin());
        //1.2.如果车辆基础信息不为空
        if (vehicleInfoModel != null) {
            //1.2.1.将车系seriesName，车型modelName,年限LiveTime,销售日期saleDate，车辆类型carType封装到onlineDataObj对象中
            value.setModelName(vehicleInfoModel.getModelName());
            value.setSeriesName(vehicleInfoModel.getSeriesName());
            value.setLiveTime(vehicleInfoModel.getLiveTime());
            value.setModelName(vehicleInfoModel.getModelName());
            //1.2.2.将onlineDataObj收集返回
            out.collect(value);
        }else{
            //1.3.打印输出，基础信息不存在
            System.out.println("当前车辆的基础信息不存在，请查看！" + value.getVin());
        }
    }

    /**
     * 处理广播中的元素
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(HashMap<String, VehicleInfoModel> value, BroadcastProcessFunction<OnlineDataObj,
            HashMap<String, VehicleInfoModel>, OnlineDataObj>.Context ctx, Collector<OnlineDataObj> out) throws Exception {
        BroadcastState<String, VehicleInfoModel> broadcastState = ctx.getBroadcastState(vehicleInfoModelDesc);
        //清空广播变量
        broadcastState.clear();
        //将最新的广播变量更新进去
        broadcastState.putAll(value);
    }
}
