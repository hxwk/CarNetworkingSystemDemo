package cn.itcast.flink.streaming.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class VehicleLocationModel implements Serializable {
    //国家
    private String country;
    //省份
    private String province;
    //城市
    private String city;
    //区县
    private String district;
    //详细地址
    private String address;
    //纬度
    private Double lat = -999999D;
    //经度
    private Double lng = -999999D;
}