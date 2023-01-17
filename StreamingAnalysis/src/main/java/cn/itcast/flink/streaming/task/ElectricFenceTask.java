package cn.itcast.flink.streaming.task;

import cn.itcast.flink.streaming.entity.ElectricFenceModel;
import cn.itcast.flink.streaming.entity.ElectricFenceResultTmp;
import cn.itcast.flink.streaming.entity.ItcastDataPartObj;
import cn.itcast.flink.streaming.process.ElectricFenceModelFunction;
import cn.itcast.flink.streaming.process.ElectricFenceRulesFuntion;
import cn.itcast.flink.streaming.sink.ElectricFenceMysqlSink;
import cn.itcast.flink.streaming.source.MysqlElectricFenceResultSource;
import cn.itcast.flink.streaming.source.MysqlElectricFenceSouce;
import cn.itcast.flink.streaming.util.JsonParsePartUtil;
import cn.itcast.flink.streaming.window.process.ElectricFenceWindowFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.datanucleus.util.StringUtils;

import java.util.HashMap;

/**
 * Author itcast
 * Date 2022/6/9 15:15
 * Desc 需求 - 通过车辆每5s上报的数据与电子围栏的数据（静态的）进行关联操作
 * 得到一个对象包含实时上报的车辆数据和关联vin的电子围栏数据，计算每 90 一个窗口内
 * 在圈内的数据的个数大于/小于等于在圈外的数据，如果在圈内的多，认为在电子栅栏内，否则在电子栅栏外
 * 如何计算在圈内还是圈外？ 球面距离的计算
 * 在同一个窗口内 90s 计算圈内和圈外的个数比较
 * 进栅栏还是出栅栏？ 如果上个窗口在栅栏内，当前窗口在栅栏外，出栅栏，否则是进栅栏
 */
public class ElectricFenceTask extends BaseTask {
    public static void main(String[] args) throws Exception {
        // 1）初始化flink流处理的运行环境
        StreamExecutionEnvironment env = getEnv(ElectricFenceTask.class.getSimpleName());
        // 2）读取kafka数据源（调用父类的方法）
        DataStreamSource<String> source = null;
        try {
            source = getSource(env, SimpleStringSchema.class, "__consumer_electric_fence_");
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        // 3）将字符串转换成javaBean（ItcastDataPartObj）对象
        SingleOutputStreamOperator<ItcastDataPartObj> vehicleDataStream = source.map(JsonParsePartUtil::parseJsonToObject)
                // 4）过滤出来正常数据
                .filter(obj -> StringUtils.isEmpty(obj.getErrorData()));
        // 5）读取电子围栏规则数据以及电子围栏规则关联的车辆数据并进行广播
        DataStreamSource<HashMap<String, ElectricFenceResultTmp>> electricFenceStream = env.addSource(new MysqlElectricFenceSouce());
        //将电子围栏数据广播出去
        //定义广播状态的描述
        MapStateDescriptor<String, ElectricFenceResultTmp> electricFenceStateDesc =
                new MapStateDescriptor<>("electricFence", Types.STRING, Types.POJO(ElectricFenceResultTmp.class));
        //广播出去
        BroadcastStream<HashMap<String, ElectricFenceResultTmp>> electricFenceBroadcast = electricFenceStream
                .broadcast(electricFenceStateDesc);
        // 6）将原始数据（消费的kafka数据）与电子围栏规则数据进行关联操作（Connect）并flatMap为 ElectricFenceRulesFuntion
        SingleOutputStreamOperator<ElectricFenceModel> processVehicleElectricFenceStream = vehicleDataStream.connect(electricFenceBroadcast)
                .process(new ElectricFenceRulesFuntion());
        // 7）对上步数据分配水印（30s）并根据 vin 分组后应用90s滚动窗口，然后对窗口进行自定义函数的开发（计算出来该窗口的数据属于电子围栏外还是电子围栏内）
        SingleOutputStreamOperator<ElectricFenceModel> windowProcessStream = processVehicleElectricFenceStream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ElectricFenceModel>(Time.seconds(30)) {
                    @Override
                    public long extractTimestamp(ElectricFenceModel element) {
                        return element.getTerminalTimestamp();
                    }
                }).keyBy(obj -> obj.getVin())
                .window(TumblingEventTimeWindows.of(Time.seconds(90)))
                .process(new ElectricFenceWindowFunction());

        // 8）读取电子围栏分析结果表的数据并进行广播
        MapStateDescriptor<String, Long> electricFenceResultStateDesc = new MapStateDescriptor<>(
                "electricFenceResultState",
                Types.STRING,
                Types.LONG
        );

        BroadcastStream<HashMap<String, Long>> electricFenceResultBroadcastStream = env
                .addSource(new MysqlElectricFenceResultSource())
                .broadcast(electricFenceResultStateDesc);
        // 9）对第七步和第八步产生的数据进行关联操作（connect）
        SingleOutputStreamOperator<ElectricFenceModel> resultStream = windowProcessStream
                .connect(electricFenceResultBroadcastStream)
                // 10）对第九步的结果进行滚动窗口操作，应用自定义窗口函数（实现添加uuid和inMysql属性赋值）
                .process(new ElectricFenceModelFunction());
        // 11）将分析后的电子围栏结果数据实时写入到mysql数据库中
        resultStream.addSink(new ElectricFenceMysqlSink());
        // 12）运行作业，等待停止
        env.execute();
    }
}
