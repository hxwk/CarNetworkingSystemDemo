package cn.itcast.flink.streaming.process;

import cn.itcast.flink.streaming.entity.ElectricFenceModel;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * Author itcast
 * Date 2022/6/10 11:37
 * Desc 此类主要是将结果表中的 id 和 isMySQL是否在数据库中存在 merge 合并到 ElectricFenceModel 中
 */
public class ElectricFenceModelFunction extends BroadcastProcessFunction<ElectricFenceModel, HashMap<String,Long>,ElectricFenceModel> {
    MapStateDescriptor<String, Long> electricFenceResultStateDesc = new MapStateDescriptor<>(
            "electricFenceResultState",
            Types.STRING,
            Types.LONG
    );
    @Override
    public void processElement(ElectricFenceModel value, BroadcastProcessFunction<ElectricFenceModel, HashMap<String, Long>, ElectricFenceModel>.ReadOnlyContext ctx, Collector<ElectricFenceModel> out) throws Exception {
        //1.1.通过getvin获取配置流中是否存在值
        Long id = ctx.getBroadcastState(electricFenceResultStateDesc).get(value.getVin());
        //2.如果不为 null
        if(id != null){
            //2.1.设置 uuid 为当前时间戳
            value.setUuid(value.getTerminalTimestamp());
            //2.2.设置库中InMysql是否存在为 true
            //后续插入数据到mysql，确认当前应该使用 insert 还是 update
            value.setInMysql(true);
        }else{         //3.否则
            //3.1.设置 uuid 为最大值-当前时间戳
            value.setUuid(Long.MAX_VALUE - System.currentTimeMillis());
            //3.2 设置库中是否存在为 false
            value.setInMysql(false);
        }
        out.collect(value);
    }

    @Override
    public void processBroadcastElement(HashMap<String, Long> value, BroadcastProcessFunction<ElectricFenceModel,
            HashMap<String, Long>, ElectricFenceModel>.Context ctx, Collector<ElectricFenceModel> out) throws Exception {
        BroadcastState<String, Long> broadcastState = ctx.getBroadcastState(electricFenceResultStateDesc);
        //清空广播变量，将最新的车辆电子围栏结果表的数据放进来
        broadcastState.clear();
        broadcastState.putAll(value);
    }
}
