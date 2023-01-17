package cn.itcast.flink.streaming.task;

import cn.itcast.flink.streaming.entity.ItcastDataPartObj;
import cn.itcast.flink.streaming.entity.OnlineDataObj;
import cn.itcast.flink.streaming.entity.VehicleInfoModel;
import cn.itcast.flink.streaming.map.LocationInfoRedisFunction;
import cn.itcast.flink.streaming.process.VehicleInfoMapMysqlFunction;
import cn.itcast.flink.streaming.sink.OnlineStatisticsMysqlSink;
import cn.itcast.flink.streaming.source.VehicleInfoMysqlSource;
import cn.itcast.flink.streaming.sync.AsyncHttpQueryFunction;
import cn.itcast.flink.streaming.util.JsonParsePartUtil;
import cn.itcast.flink.streaming.window.process.OnlineStatisticsWindowFunction;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

/**
 * Author itcast
 * Date 2022/6/10 16:16
 * Desc 需求 - 在线告警分析需求
 * 需要三部分数据：
 * 1. 车辆的实时上报数据（故障告警的信息）
 * 2. 车辆的静态数据（车型车系、购买车辆的时间）
 * 3. 通过经纬度获取到的国家省市区等静态数据
 * 4. 将结果数据写入到 mysql 中
 */
public class OnlineStatisticsTask extends BaseTask {
    public static void main(String[] args) throws Exception {
        //1）初始化flink流处理的运行环境（事件时间、checkpoint、hadoop name）
        StreamExecutionEnvironment env = getEnv(OnlineStatisticsTask.class.getSimpleName());
        //2）接入kafka数据源，消费kafka数据
        DataStreamSource<String> source = getSource(env, SimpleStringSchema.class, "__consumer_online_statistics_");
        //3）将消费到的json字符串转换成ItcastDataPartObj对象
        SingleOutputStreamOperator<ItcastDataPartObj> vehicleSourceStream = source.map(JsonParsePartUtil::parseJsonToObject)
                //4）过滤掉异常数据，保留正常数据
                .filter(new FilterFunction<ItcastDataPartObj>() {
                    @Override
                    public boolean filter(ItcastDataPartObj value) throws Exception {
                        return StringUtils.isEmpty(value.getErrorData());
                    }
                });
        //5）与redis维度表进行关联拉宽地理位置信息，对拉宽后的流数据关联redis，根据geohash找到地理位置信息，进行拉宽操作
        SingleOutputStreamOperator<ItcastDataPartObj> redisLocationInfoStream = vehicleSourceStream
                .map(new LocationInfoRedisFunction());
        //定义两个侧输出流
        OutputTag<ItcastDataPartObj> withLocation = new OutputTag<>(
                "withLocation",
                Types.POJO(ItcastDataPartObj.class)
        );

        OutputTag<ItcastDataPartObj> withoutLocation = new OutputTag<>(
                "withoutLocation",
                Types.POJO(ItcastDataPartObj.class)
        );

        //6）过滤出来redis拉宽成功的地理位置数据
        SingleOutputStreamOperator<ItcastDataPartObj> processVehicleStream = redisLocationInfoStream
                .process(new ProcessFunction<ItcastDataPartObj, ItcastDataPartObj>() {
                    @Override
                    public void processElement(ItcastDataPartObj value, ProcessFunction<ItcastDataPartObj, ItcastDataPartObj>.Context ctx, Collector<ItcastDataPartObj> out) throws Exception {
                        if (value.getDistrict() != null && value.getProvince() != null && value.getCity() != null) {
                            ctx.output(withLocation,value);
                        }else{
                            ctx.output(withoutLocation,value);
                        }
                    }
                });
        //拉宽成功的位置信息的数据流
        DataStream<ItcastDataPartObj> withLocationStream = processVehicleStream.getSideOutput(withLocation);
        //7）过滤出来redis拉宽失败的地理位置数据
        DataStream<ItcastDataPartObj> withoutLocationStream = processVehicleStream.getSideOutput(withoutLocation);
        //8）对redis拉宽失败的地理位置数据使用异步io访问高德地图逆地理位置查询地理位置信息，并将返回结果写入到redis中
        //高德Api io访问怎么用
        //https://restapi.amap.com/v3/geocode/regeo?key=01b2cb433bf65d3dacac9b059959b70f&location=113.923214,22.57574
        //异步数据流 Flink AsyncDataStream
        //使用到 AsyncFuture 异步获取数据流，等待拿到的结果 get 并返回
        SingleOutputStreamOperator<ItcastDataPartObj> withGaodeItcastDataPartObj = AsyncDataStream.unorderedWait(
                withoutLocationStream,
                new AsyncHttpQueryFunction(),
                30 * 1000,
                TimeUnit.MILLISECONDS
        );
        //9）将reids拉宽的地理位置数据与高德api拉宽的地理位置数据进行合并
        DataStream<ItcastDataPartObj> withLocationVehicleStream = withLocationStream.union(withGaodeItcastDataPartObj);
        //10）分配水印和根据vin进行分流，创建原始数据的30s的滚动窗口
        SingleOutputStreamOperator<OnlineDataObj> alarmVehicleDataStream = withLocationVehicleStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ItcastDataPartObj>(Time.seconds(3)) {
                    @Override
                    public long extractTimestamp(ItcastDataPartObj element) {
                        return element.getTerminalTimeStamp();
                    }
                })
                .keyBy(obj -> obj.getVin())
                .window(TumblingEventTimeWindows.of(Time.seconds(30)))
                //11）对原始数据的窗口流数据进行实时故障分析（区分出来告警数据和非告警数据19个告警字段）
                .process(new OnlineStatisticsWindowFunction());

        //定义广播变量的状态描述
        MapStateDescriptor<String, VehicleInfoModel> vehicleInfoModelDesc = new MapStateDescriptor<>(
                "vehicleInfoModel",
                Types.STRING,
                Types.POJO(VehicleInfoModel.class)
        );
        //12）加载业务中间表（7张表：车辆表、车辆类型表、车辆销售记录表，车俩用途表4张），并进行广播
        DataStreamSource<HashMap<String, VehicleInfoModel>> vehicleInfoModelStream = env.addSource(new VehicleInfoMysqlSource());
        BroadcastStream<HashMap<String, VehicleInfoModel>> vehicleInfoBroadcastStream = vehicleInfoModelStream.broadcast(vehicleInfoModelDesc);
        //13）将第11步和第12步的广播流结果进行关联，并应用拉宽操作
        SingleOutputStreamOperator<OnlineDataObj> result = alarmVehicleDataStream.connect(vehicleInfoBroadcastStream)
                .process(new VehicleInfoMapMysqlFunction());
        //14）将拉宽后的结果数据写入到mysql数据库中
        result.addSink(new OnlineStatisticsMysqlSink());
        //15）启动作业
        env.execute();
    }
}
