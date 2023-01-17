package cn.itcast.flink.streaming.task;

import cn.itcast.flink.streaming.entity.ItcastDataObj;
import cn.itcast.flink.streaming.sink.SrcDataToHBaseOptimizerSink;
import cn.itcast.flink.streaming.sink.SrcDataToHBaseSink;
import cn.itcast.flink.streaming.sink.SrcDetailDataToHBaseSink;
import cn.itcast.flink.streaming.util.JsonParseUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Author itcast
 * Date 2022/6/5 9:41
 * Desc 需求 - 主要用于读取kafka集群中车辆的json数据，将其进行解析、转换、过滤，最终将过滤出来的
 * 正确数据保存到 hdfs 和 hbase 上，将错误的数据保存到 hdfs 上
 */
public class KafkaSourceDataTask extends BaseTask {
    public static void main(String[] args) throws Exception {

        //todo 1.创建流执行环境
        StreamExecutionEnvironment env = getEnv("KafkaSourceDataTask");
        //获取 kafka 的数据源
        DataStreamSource<String> source = getSource(env, SimpleStringSchema.class, "__consumer_vehicle_data_");
        //todo 7 打印输出
        source.print();
        //todo 8 将读取出来的 json 字符串转换成 ItcastDataObj
        SingleOutputStreamOperator<ItcastDataObj> itcastDataObjStream = source.map(new MapFunction<String, ItcastDataObj>() {
            @Override
            public ItcastDataObj map(String value) throws Exception {
                if (StringUtils.isNotEmpty(value)) {
                    return JsonParseUtil.parseJsonToObject(value);
                } else {
                    return null;
                }
            }
        });

        itcastDataObjStream.print();
        //todo 9 将数据拆分成正确的数据和异常的数据
        //将获取到的车辆数据，判断对象中 vin 和 terminalTime 两个字段是否为空，如果为空说明数据异常，否则正常。
        //使用侧输出流来实现正确数据和错误数据
        //1.定义侧输出流 OutputTag
        OutputTag<ItcastDataObj> srcData = new OutputTag<>(
                "src_data",
                Types.POJO(ItcastDataObj.class)
        );

        OutputTag<ItcastDataObj> errorData = new OutputTag<>(
                "error_data",
                Types.POJO(ItcastDataObj.class)
        );
        //2.处理数据流，ProcessFunction ，将满足特定条件的数据放在不同的 OUtputTag 中
        SingleOutputStreamOperator<ItcastDataObj> vehicleSourceStream = itcastDataObjStream
                .process(new ProcessFunction<ItcastDataObj, ItcastDataObj>() {
                    @Override
                    public void processElement(ItcastDataObj value, ProcessFunction<ItcastDataObj, ItcastDataObj>.Context ctx, Collector<ItcastDataObj> out) throws Exception {
                        //ErrorData 作用就是如果为空，就认为当前数据是有效数据
                        //如果 ErrorData 不为空，就认为当前数据是无效数据
                        //ErrorData 是如何来的？
                        // ① 解析失败了，认为当前的ErrorData = Json字符串
                        // ② 如果当前vin和terminalTime 为空， ErrorData = Json字符串
                        if (StringUtils.isNotEmpty(value.getErrorData())) {
                            ctx.output(errorData, value);
                        } else {
                            ctx.output(srcData, value);
                        }
                    }
                });

        //todo 10 将正确的数据保存到 hdfs StreamingFileSink
        //StreamFileSink
        //输出文件的前后缀样式
        final StreamingFileSink<String> srcSink = getStreamingFileSink(
                "/apps/hive/warehouse/ods.db/itcast_src",
                "yyyy-MM-dd",
                "src_",
                ".txt"
        );

        final StreamingFileSink<String> errorSink = getStreamingFileSink(
                "/apps/hive/warehouse/ods.db/itcast_error",
                "yyyy-MM-dd",
                "error_",
                ".txt"
        );

        /*vehicleSourceStream.getSideOutput(srcData)
                .map(ItcastDataObj::toHiveString)
                .addSink(srcSink);*/
        //todo 11 将错误的数据保存到 hdfs 上
        /*vehicleSourceStream.getSideOutput(errorData)
                .map(ItcastDataObj::toHiveString)
                .addSink(errorSink);*/
        //todo 12 将正确的数据写入到 hbase 中
       /* vehicleSourceStream.getSideOutput(srcData)
                .addSink(new SrcDataToHBaseSink("itcast_src"));*/

        //vehicleSourceStream.getSideOutput(srcData).addSink(new SrcDataToHBaseOptimizerSink("itcast_src_fastdiff"));
        //itcast_src_fastdiff_gz
        //itcast_src_fastdiff_lz4
        //itcast_src_fastdiff_snappy
        /*vehicleSourceStream.getSideOutput(srcData)
                .addSink(new SrcDataToHBaseOptimizerSink("itcast_src_fastdiff_gz"));
        vehicleSourceStream.getSideOutput(srcData)
                .addSink(new SrcDataToHBaseOptimizerSink("itcast_src_fastdiff_snappy"));
        vehicleSourceStream.getSideOutput(srcData).addSink(new SrcDataToHBaseOptimizerSink("itcast_src_fastdiff_lz4"));*/
        /*vehicleSourceStream.getSideOutput(srcData)
                .addSink(new SrcDetailDataToHBaseSink("itcastsrc_vehicle_detail"));*/

        //todo 13 执行流环境
        env.execute();
    }
}
