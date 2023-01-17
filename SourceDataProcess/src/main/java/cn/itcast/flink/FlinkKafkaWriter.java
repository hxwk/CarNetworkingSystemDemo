package cn.itcast.flink;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Author itcast
 * Date 2022/6/4 15:00
 * Desc 需求 - 模拟车辆数据上报到大数据的 kafka 集群，
 * 就是将文件中的数据写入到 kafka 集群中
 */
public class FlinkKafkaWriter {
    public static void main(String[] args) throws Exception {
        //开发步骤：
        //1.获取流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置并行度等参数
        env.setParallelism(1);
        env.enableCheckpointing(20000);
        //env.setStateBackend(new FsStateBackend("hdfs://node01:8020/flink-checkpoints"));
        //设置当前使用 Event-Time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //3.读取文件数据源
        DataStreamSource<String> source = env.readTextFile("source/sourcedata.txt");
        //4.转换操作
        //5.将数据写入到kafka
        //初始化FlinkKafkaConsumer，设置相关集群参数
        Properties producerConfig = new Properties();
        producerConfig.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092,node02:9092,node03:9092");
        producerConfig.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, 5 + "");

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "vehiclesource",
                new KafkaSerializationSchema<String>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {

                        return new ProducerRecord<byte[],byte[]>("vehicledata", element.getBytes());
                    }
                },
                producerConfig,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        //将数据源写入
        source.addSink(producer);

        //6.执行流环境
        env.execute();
        //7.查验 kafka 中是否有数据的存在
        // [root@node1 kafka]# bin/kafka-console_consumer.sh --bootstrap-server node01:9092 --topic vehiclesource
    }
}
