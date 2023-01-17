package cn.itcast.flink.streaming.map;

import cn.itcast.flink.streaming.entity.ItcastDataPartObj;
import cn.itcast.flink.streaming.entity.VehicleLocationModel;
import cn.itcast.flink.streaming.util.GeoHashUtil;
import cn.itcast.flink.streaming.util.RedisUtil;
import com.alibaba.fastjson.JSON;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Author itcast
 * Date 2022/6/10 16:30
 * 此类主要用于拉宽位置信息的数据
 * 读取实时上报的车辆数据经纬度，将经纬度转换成 国家省市区，通过 redis 数据库获取
 */
public class LocationInfoRedisFunction extends RichMapFunction<ItcastDataPartObj, ItcastDataPartObj> {
    @Override
    public ItcastDataPartObj map(ItcastDataPartObj value) throws Exception {
        //1.1.获取车辆数据的经度和维度生成 geohash
        // 经度和维度 -> geohash
        String geoHash = GeoHashUtil.encode(value.getLat(), value.getLng());
        //1.2.根据 geohash 从redis中获取value值（geohash在redis中是作为主键存在）
        // Redis 有五种数据结构 ： String Hash Set List ZSet ，key-value 键值对存储
        // redis 中存储的 value 是一个 json 字符串
        //
        byte[] locationBytes = RedisUtil.get(Bytes.toBytes(geoHash));
        //1.3.如果查询出来的值不为空，将其通过JSON对象解析成 VehicleLocationModel 对象，否则置为 null
        if (locationBytes != null) {
            VehicleLocationModel vehicleLocationModel = JSON
                    .parseObject(Bytes.toString(locationBytes), VehicleLocationModel.class);
            //将位置数据赋值给车辆数据
            BeanUtils.copyProperties(value,vehicleLocationModel);
        }else{
            //1.4.如果当前对象不为空，将国家，省市区地址赋值给 itcastDataPartObj，否则置为 null
            value.setCountry(null);
            value.setProvince(null);
            value.setCity(null);
            value.setDistrict(null);
            value.setAddress(null);
        }
        //1.5.返回数据
        return value;
    }
}
