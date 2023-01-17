package cn.itcast.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Author itcast
 * Date 2022/6/4 16:57
 * Desc 需求 - Flink读取kafka集群中指定topic的vehiclesource数据
 */
public class FlinkKafkaRead {
    public static void main(String[] args) throws Exception {
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置并行度等参数
        env.setParallelism(1);
        env.enableCheckpointing(5000);
        //3.初始化FlinkKafkaConsumer及相关参数
        //定义参数
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"node01:9092,node02:9092,node03:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"__consumer_vehicle_source");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false+"");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "vehiclesource",
                new SimpleStringSchema(),
                props
        );
        //4.设置Consumer提交offset 到checkpoint
        consumer.setCommitOffsetsOnCheckpoints(true);
        consumer.setStartFromEarliest();
        //5.生成数据源
        DataStreamSource<String> source = env.addSource(consumer);
        //6.打印数据源
        source.print();
        //7.执行流环境
        env.execute();

    }
}
