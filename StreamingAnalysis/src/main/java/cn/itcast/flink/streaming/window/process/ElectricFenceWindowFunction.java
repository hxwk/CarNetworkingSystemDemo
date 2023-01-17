package cn.itcast.flink.streaming.window.process;

import cn.itcast.flink.streaming.entity.ElectricFenceModel;
import com.clearspring.analytics.util.Lists;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Author itcast
 * Date 2022/6/10 9:46
 * 此类功能
 * 1. 判断 90s 窗口内在圈内的数量和圈外的数量比较，如果圈内的多说明在电子围栏内，否则在电子围栏外
 * 2. 获取上个窗口内的电子围栏的状态，如果和当前围栏的状态不同，
 * 2.1 如果上次在电子围栏外，当前是在电子围栏内，进栅栏，同时进栅栏告警
 * 2.2 如果上次在电子围栏内，当前是在电子围栏外，出栅栏，同时出栅栏告警
 * <p>
 * 3. 技术点：①统计圈内或圈外的和：for遍历  ②state状态管理  ③state的生命周期 TTL
 */
public class ElectricFenceWindowFunction extends ProcessWindowFunction<ElectricFenceModel, ElectricFenceModel, String, TimeWindow> {

    //1.定义存储历史电子围栏数据的state，<vin，是否在电子围栏内0:内，1:外> MapState<String, Integer>
    MapState<String, Integer> lastPeriodState = null;

    /**
     * 初始化状态，此状态用于保存上个窗口内的状态数据和状态 90 TTL
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        //2.1 定义mapState的描述器（相当于表结构） <String，Integer> [vin,0或1]
        MapStateDescriptor<String, Integer> lastPeriodStateDesc = new MapStateDescriptor<>(
                "lastPeriodState",
                Types.STRING,
                Types.INT
        );
        //2.2 获取 parameterTool，用来读取配置文件参数
        ParameterTool pt = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        //2.3 读取状态的超时时间 "vehicle.state.last.period" ,构建ttl设置更新类型和状态可见
        String lastPeriod = pt.get("vehicle.state.last.period");
        //2.4 设置状态描述 StateTtlConfig，开启生命周期时间
        StateTtlConfig config = StateTtlConfig.newBuilder(Time.seconds(Long.parseLong(lastPeriod)))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        //2.5 获取map状态
        lastPeriodStateDesc.enableTimeToLive(config);
        lastPeriodState = getRuntimeContext().getMapState(lastPeriodStateDesc);
    }

    /**
     * 处理每个车辆窗口90内的数据，计算出来当前电子围栏的告警和状态信息
     *
     * @param vin
     * @param context
     * @param elements
     * @param out
     * @throws Exception
     */
    @Override
    public void process(String vin, ProcessWindowFunction<ElectricFenceModel, ElectricFenceModel, String, TimeWindow>.Context context,
                        Iterable<ElectricFenceModel> elements,
                        Collector<ElectricFenceModel> out) throws Exception {
        //1.创建返回对象
        ElectricFenceModel model = new ElectricFenceModel();
        //2.对窗口内的数据进行排序
        List<ElectricFenceModel> electricFenceModels = Lists.newArrayList(elements);
        Collections.sort(electricFenceModels, Comparator.comparing(ElectricFenceModel::getTerminalTimestamp));
        //3.从 state 中获取车辆vin对应的上一次窗口电子围栏lastStateValue标记（车辆上一次窗口是否在电子围栏中）0：电子围栏内 1：电子围栏外
        Integer lastPeriodStateValue = lastPeriodState.get(vin);
        //4.如果上次状态为空，初始化赋值
        if (lastPeriodStateValue == null) {
            lastPeriodStateValue = -999999;
        }
        //5.判断当前处于电子围栏内还是电子围栏外
        //5.1.定义当前车辆电子围栏内出现的次数
        Long inCount = electricFenceModels.stream()
                .filter(obj -> obj.getNowStatus() == 0)
                .count();
        //5.2.定义当前车辆电子围栏外出现的次数
        long outCount = electricFenceModels.stream()
                .filter(obj -> obj.getNowStatus() == 1)
                .count();
        //6.定义当前窗口的电子围栏状态
        int currentState = -999999;
        //7. 90s内车辆出现在电子围栏内的次数多于出现在电子围栏外的次数，则认为当前状态处于电子围栏内
        if (inCount >= outCount) {
            currentState = 0;
        } else {
            currentState = 1;
        }
        //8. 将当前窗口的电子围栏状态写入到 state 中，供下次判断
        lastPeriodState.remove(vin);
        lastPeriodState.put(vin, currentState);
        //9.如果当前电子围栏状态与上一次电子围栏状态不同
        //9.1.如果上一次窗口处于电子围栏外，而本次是电子围栏内，则将进入电子围栏的时间写入到数据库中
        if (lastPeriodStateValue == 0 && currentState == 1) {
            //9.1.1.过滤出来状态为0的第一条数据
            ElectricFenceModel firstInElectricFence = electricFenceModels
                    .stream()
                    .filter(obj -> obj.getNowStatus() == 0).findFirst().get();
            //9.1.2.拷贝属性给 electricFenceModel 并将进入终端时间赋值，并且将状态告警字段赋值为1 0:出围栏 1:进围栏，将数据collect返回
            //对象拷贝
            BeanUtils.copyProperties(model, firstInElectricFence);
            //告警字段
            model.setStatusAlarm(1);
            model.setStatusAlarmMsg("欢迎光临，进入栅栏");
        }        //9.2.如果上一次窗口处于电子围栏内，而本次是电子围栏外，则将出电子围栏的时间写入到数据库中,以最后一条时间为准
        else if (lastPeriodStateValue == 1 && currentState == 0) {
            //9.2.1.过滤出来状态倒序为1的第一条数据
            ElectricFenceModel lastOutElectricFence = electricFenceModels
                    .stream()
                    .filter(obj -> obj.getNowStatus() == 1)
                    .sorted(Comparator.reverseOrder())
                    .findFirst().get();
            //9.2.2.拷贝属性给 electricFenceModel 并将出终端时间赋值，并且将状态告警 0:出围栏 1:进围栏，将数据collect返回
            BeanUtils.copyProperties(model,lastOutElectricFence);
            //告警字段
            model.setNowStatus(0);
            model.setStatusAlarmMsg("欢迎下次光临，已出栅栏！");
        }
        //收集最终的结果数据
        out.collect(model);
    }
}
