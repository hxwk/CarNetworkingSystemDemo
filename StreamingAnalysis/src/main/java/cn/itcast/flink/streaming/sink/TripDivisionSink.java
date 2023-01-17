package cn.itcast.flink.streaming.sink;

import cn.itcast.flink.streaming.entity.TripModel;
import cn.itcast.flink.streaming.util.DateFormatDefine;
import cn.itcast.flink.streaming.util.DateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Author itcast
 * Date 2022/6/5 17:34
 * Desc 此类主要用于将过滤出来正确的车辆的数据写入到 hbase 表中
 * RichSinkFunction<ItcastDataObj>
 * 对当前的写出HBase类进行优化操作，将写入到 HBase 表中的数据进行优化
 * 将一个批次的数据先写入 Mutator ，达到一定数据量或时间之后，将 Mutator 中的数据提交 HBase
 * 使用到的对象 BufferMutator ：
 * mutator ： 提交任务 和 flush ： 强制刷新
 */
public class TripDivisionSink extends RichSinkFunction<TripModel> {
    final Logger logger = LoggerFactory.getLogger(this.getClass().getSimpleName());
    //定义变量，变量存储到hbase 的表名
    private String tableName;
    //定义连接对象
    private Connection conn;
    //定义表对象
    //private Table table;
    private BufferedMutator bufferedMutator;

    //2.创建一个有参数-表名的构造方法
    public TripDivisionSink(String _tableName) {
        this.tableName = _tableName;
    }

    //3.重写open方法
    @Override
    public void open(Configuration parameters) throws Exception {
        //3.1 从上下文获取到全局的参数
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        //3.2 设置hbase的配置，Zookeeper Quorum集群和端口和TableInputFormat的输入表
        String zkServer = parameterTool.get("zookeeper.quorum");
        String clientPort = parameterTool.get("zookeeper.clientPort");
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set(HConstants.ZOOKEEPER_QUORUM, zkServer);
        conf.set(HConstants.CLIENT_ZOOKEEPER_CLIENT_PORT, clientPort);
        conf.set(TableInputFormat.INPUT_TABLE, tableName);
        //3.3 通过连接工厂创建连接
        conn = ConnectionFactory.createConnection(conf);
        //3.4 通过连接获取表对象
        //table = conn.getTable(TableName.valueOf(tableName));
        //定义 Mutator 参数
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName));
        //将缓存周期刷写到 HBase 的超时时间
        params.setWriteBufferPeriodicFlushTimeoutMs(1000 * 10);
        params.writeBufferSize(10 * 1024);

        bufferedMutator = conn.getBufferedMutator(params);
    }

    //5. 重写 invoke 方法，将读取的数据写入到 hbase
    @Override
    public void invoke(TripModel value, Context context) throws Exception {
        //5.1 setDataSourcePut输入参数value，返回put对象
        Put put = setDataSourcePut(value);
        //5.2 将 Put 对象直接保存到 HBase 中
        bufferedMutator.mutate(put);
    }

    @Override
    public void close() throws Exception {
        if (bufferedMutator != null) bufferedMutator.close();
        if (!conn.isClosed()) conn.close();
    }

    //6. 实现 setDataSourcePut 方法
    private Put setDataSourcePut(TripModel tripModel) {
        //生成rowkey
        String rowkey = tripModel.getVin() + StringUtils.reverse(tripModel.getTripStartTime());
        //定义 columnFamily
        String cf ="cf";
        //定义 key-value pair
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("vin"), Bytes.toBytes(tripModel.getVin()));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lastSoc"), Bytes.toBytes(String.valueOf(tripModel.getLastSoc())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("lastMileage"), Bytes.toBytes(String.valueOf(tripModel.getLastMileage())));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("tripStartTime"), Bytes.toBytes(tripModel.getTripStartTime()));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("start_BMS_SOC"), Bytes.toBytes(String.valueOf(tripModel.getStart_BMS_SOC())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("start_longitude"), Bytes.toBytes(String.valueOf(tripModel.getStart_longitude())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("start_latitude"), Bytes.toBytes(String.valueOf(tripModel.getStart_latitude())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("start_mileage"), Bytes.toBytes(String.valueOf(tripModel.getStart_mileage())));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("end_BMS_SOC"), Bytes.toBytes(String.valueOf(tripModel.getEnd_BMS_SOC())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("end_longitude"), Bytes.toBytes(String.valueOf(tripModel.getEnd_longitude())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("end_latitude"), Bytes.toBytes(String.valueOf(tripModel.getEnd_latitude())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("end_mileage"), Bytes.toBytes(String.valueOf(tripModel.getEnd_mileage())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("tripEndTime"), Bytes.toBytes(tripModel.getTripEndTime()));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("mileage"), Bytes.toBytes(String.valueOf(tripModel.getMileage())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("max_speed"), Bytes.toBytes(String.valueOf(tripModel.getMax_speed())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("soc_comsuption"), Bytes.toBytes(String.valueOf(tripModel.getSoc_comsuption())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("time_comsuption"), Bytes.toBytes(String.valueOf(tripModel.getTime_comsuption())));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("total_low_speed_nums"), Bytes.toBytes(String.valueOf(tripModel.getTotal_low_speed_nums())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("total_medium_speed_nums"), Bytes.toBytes(String.valueOf(tripModel.getTotal_medium_speed_nums())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("total_high_speed_nums"), Bytes.toBytes(String.valueOf(tripModel.getTotal_high_speed_nums())));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("Low_BMS_SOC"), Bytes.toBytes(String.valueOf(tripModel.getLow_BMS_SOC())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("Medium_BMS_SOC"), Bytes.toBytes(String.valueOf(tripModel.getMedium_BMS_SOC())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("High_BMS_SOC"), Bytes.toBytes(String.valueOf(tripModel.getHigh_BMS_SOC())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("Low_BMS_Mileage"), Bytes.toBytes(String.valueOf(tripModel.getLow_BMS_Mileage())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("Medium_BMS_Mileage"), Bytes.toBytes(String.valueOf(tripModel.getMedium_BMS_Mileage())));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("High_BMS_Mileage"), Bytes.toBytes(String.valueOf(tripModel.getHigh_BMS_Mileage())));

        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("tripStatus"), Bytes.toBytes(String.valueOf(tripModel.getTripStatus())));
        //添加处理时间 processTime
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("processTime"), Bytes.toBytes(DateUtil.getCurrentDateTime(DateFormatDefine.DATE_TIME_FORMAT)));
        return put;
    }
}
