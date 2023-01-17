package cn.itcast.flink.streaming.task;

import cn.itcast.flink.streaming.util.ConfigLoader;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Author itcast
 * Date 2022/6/5 14:39
 * Desc TODO
 * 类的修饰符
 * public - 所有的程序、包、类都可以直接访问
 * private - 私有的，只能访问本类内的方法
 * protected - 受保护的，继承这个类的所有子类可以访问
 * <p>
 * 优化的点：
 * 1.将公共的方法抽取出来放到指定的类中
 * 2.生成环境变量
 * 3.获取kafka数据源
 * 4.写入到HDFS connector
 */
public class BaseTask {
    static ParameterTool parameterTool = null;

    static {
        try {
            parameterTool = ParameterTool.fromPropertiesFile(
                    BaseTask.class.getClassLoader().getResourceAsStream("conf.properties")
            );
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取流环境的环境变量，主要用于执行Flink流处理程序
     *
     * @return
     */
    protected static StreamExecutionEnvironment getEnv(String jobName) {
        //模拟 root 用户
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //todo 2.设置并行度 ①配置文件并行度设置 ②客户端设置 flink run -p 2 ③在程序中 env.setParallel(2) ④算子上并行度（级别最高）
        env.setParallelism(1);
        //todo 3.开启checkpoint及相应的配置，最大容忍次数，最大并行checkpoint个数，checkpoint间最短间隔时间，checkpoint的最大，设置时间的属性为事件时间。
        //todo 容忍的超时时间，checkpoint如果取消是否删除checkpoint 等
        env.enableCheckpointing(10 * 1000L);
        //设定 checkpoint 时间属性 - 时间属性为事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //指定存储到 HDFS
        env.setStateBackend(new FsStateBackend(parameterTool.get("hdfsUri") + "/flink-checkpoints/" + jobName));
        //将读取到 conf.properties 所有参数作为全局 job 的参数
        env.getConfig().setGlobalJobParameters(parameterTool);

        CheckpointConfig config = env.getCheckpointConfig();
        //设置最大超时时间
        config.setCheckpointTimeout(60 * 1000);
        //设置最大并行度个数
        config.setMaxConcurrentCheckpoints(1);
        //设置最大容忍的报错次数
        config.setTolerableCheckpointFailureNumber(10);
        //程序被取消，checkpoint依然被保存下来
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //todo 4.开启重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.seconds(5)
        ));
        return env;
    }

    /**
     * 获取kafka的数据源
     *
     * @param env
     * @param clazz   反序列的类
     * @param groupId 消费者组id
     * @param <T>     传入的任意类型
     * @return 数据流
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    protected static <T> DataStreamSource<T> getSource(StreamExecutionEnvironment env, Class<? extends DeserializationSchema<T>> clazz, String groupId) throws InstantiationException, IllegalAccessException {
        //todo 5. 读取kafka中的数据
        //todo 5.1 配置参数
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigLoader.get("bootstrap.servers"));
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //todo 5.2 设置 FlinkKafkaConsumer
        FlinkKafkaConsumer<T> consumer = new FlinkKafkaConsumer<>(
                ConfigLoader.get("kafka.topic"),
                clazz.newInstance(),
                props
        );

        //todo 5.3 消费 kafka 的offset 提交给 flink 来管理
        consumer.setStartFromEarliest();
        consumer.setCommitOffsetsOnCheckpoints(true);

        //todo 6 env.addSource
        DataStreamSource<T> source = env.addSource(consumer);
        return source;
    }

    /**
     * 主要用于将 Streaming 数据写入到 HDFS 的方法
     *
     * @param path
     * @param bucketAssignerFormat
     * @param prefix
     * @param suffix
     * @return
     */
    protected static StreamingFileSink<String> getStreamingFileSink(
            String path, String bucketAssignerFormat, String prefix, String suffix
    ) {
        final StreamingFileSink<String> srcSink = StreamingFileSink
                //指定行存储还是 Bulk 存储，存储文件编码格式 GBK - UTF-8 - Latin
                .forRowFormat(new Path(
                                ConfigLoader.get("hdfsUri") + path
                        )
                        , new SimpleStringEncoder<String>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<String>("yyyy-MM-dd"))
                //设置滚筒策略，生成一个滚筒： 15分钟、间隔 5分钟、 128文件大小
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(128 * 1024 * 1024)
                                .build())
                .withOutputFileConfig(new OutputFileConfig(prefix, suffix))
                .build();
        return srcSink;
    }
}
