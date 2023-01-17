package cn.itcast.flink.streaming.task;

import cn.itcast.flink.streaming.entity.ItcastDataObj;
import cn.itcast.flink.streaming.entity.TripModel;
import cn.itcast.flink.streaming.sink.TripDivisionSink;
import cn.itcast.flink.streaming.sink.TripDriveSampleSink;
import cn.itcast.flink.streaming.util.JsonParseUtil;
import cn.itcast.flink.streaming.window.process.DriveSampleWindowFunction;
import cn.itcast.flink.streaming.window.process.DriveTripWindowFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;
import org.datanucleus.util.StringUtils;

/**
 * Author itcast
 * Date 2022/6/8 14:53
 * Desc 需求 - 计算每个驾驶行程（15分钟）内用户驾驶行为采样数据分析和驾驶行为数据分析
 * 驾驶行为采样数据： 每5s，10s，15s，20s计算每个行程的内的数据，将数据采集返回
 * 分析开发步骤：
 * 1.读取kafka中的数据，并将数据转换成 ItcastDataObj
 * 2.将驾驶行程的数据过滤出来，（chargeState=2或=3）行程数据，根据 vin 分组
 * 3.分配水印机制 watermark
 * watermark 水印机制主要解决数据延迟和数据乱序的问题
 * 车辆上报数据有可能延迟，需要分配水印
 * 如何分配： ds.assignWatermarkAndTimestamp(①单调递增 ascending ②乱序 boundedOutofOrderness)
 * 4.划分窗口 window
 * window 窗口就是将无界的数据转换成有界的数据，Flink window只标记 window start时间和 end时间，并不真正存储数据，数据
 * 被存储在 state 状态中
 * window的分类：
 * 一 时间窗口 time window
 * 1.1 tumbling time window
 * 与时间有关，每个时间窗口内的数据都不重复
 * 1.2 sliding time window
 * 与时间有关，每个时间窗口内的数据有可能重复，当滑动时间等于窗口时间，等价于滚动时间窗口
 * 1.3 session time window 【*】
 * 与时间有关，每个事件之间的时间间隔是否大于指定的时间，如果大于划分窗口
 * 二 计数窗口 count window
 * 2.1 tumbling count window
 * 2.2 sliding count window
 * 5. 驾驶行程采样分析
 * 5.1 每 5s 、 每10s 、 每15s 统计一条数据存储到数组中
 * 5.2 将数据落地到 HBase 中，最终可视化
 * 6. 驾驶行程数据分析
 * 6.1 低速
 * 6.2 中速
 * 6.3 高速
 * 用户的使用油耗 soc、里程、平均速度、切换次数等
 * 6.4 将分析的用户行为数据保存到 HBase 中供后续分析
 */
public class TripDriveTask extends BaseTask {
    public static void main(String[] args) throws Exception {
        //1.创建流执行环境
        StreamExecutionEnvironment env = getEnv(TripDriveTask.class.getSimpleName());
        //2.获取kafka中的数据
        DataStreamSource<String> vehicleSource = null;
        try {
            vehicleSource = getSource(
                    env,
                    SimpleStringSchema.class,
                    "__consumer_vehicle_trip_drive_"
            );
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        //3.将json字符串解析成车辆数据对象,将json字符串转换成 ItcastDataObj
        SingleOutputStreamOperator<ItcastDataObj> itcastDataStream = vehicleSource.map(new MapFunction<String, ItcastDataObj>() {
            @Override
            public ItcastDataObj map(String value) throws Exception {
                return JsonParseUtil.parseJsonToObject(value);
            }
        });
        //4.过滤出正确的数据并且是行程数据 chargeStatus=2或者chargeStatus=3
        //0x01:停车充电。0x02:行驶充电。0x03:未充电状态。0x04:充电完成。0xFE：异常。0xFF：无效。
        SingleOutputStreamOperator<ItcastDataObj> tripDriveDataStream = itcastDataStream.filter(
                obj -> StringUtils.isEmpty(obj.getErrorData()) && (obj.getChargeStatus() == 2 || obj.getChargeStatus() == 3)
        );
        //5.分配水印机制，设置最大延迟时间 10s
        SingleOutputStreamOperator<ItcastDataObj> watermarkStream = tripDriveDataStream.assignTimestampsAndWatermarks(
                new BoundedOutOfOrdernessTimestampExtractor<ItcastDataObj>(Time.seconds(10)) {
                    @Override
                    public long extractTimestamp(ItcastDataObj element) {
                        return element.getTerminalTimeStamp();
                    }
                }
        );
        //6.超过3分钟的数据，保存到侧输出流，分析一下数据为什么会延迟
        OutputTag<ItcastDataObj> maxOutofLatenessTag = new OutputTag<>(
                "maxOutofLateness",
                Types.POJO(ItcastDataObj.class)
        );
        //7.对车辆数据进行分组，创建会话窗口
        WindowedStream<ItcastDataObj, String, TimeWindow> windowStream = watermarkStream.keyBy(t -> t.getVin())
                .window(EventTimeSessionWindows.withGap(Time.minutes(15)))
                .allowedLateness(Time.minutes(3))
                .sideOutputLateData(maxOutofLatenessTag);
        //8.数据的采样分析
        //8.1.应用窗口，数据的采样分析
        SingleOutputStreamOperator<String[]> processStream = windowStream.process(new DriveSampleWindowFunction());
        //8.2.将分析的采样数据封装成数组，并将其保存到HBase中
        //processStream.addSink(new TripDriveSampleSink("TRIPDB:trip_samples"));
        //9.数据的行程分析
        //9.1.应用窗口数据，分析低速、中速、高速车辆的soc、行驶里程、油耗、速度、速度切换的次数等数据封装成对象
        SingleOutputStreamOperator<TripModel> tripDriveStream = windowStream.process(new DriveTripWindowFunction());
        //9.2.将这个对象保存到hbase中
        tripDriveStream.addSink(new TripDivisionSink("TRIPDB:trip_division"));
        //10.执行流环境任务
        env.execute();
    }
}
