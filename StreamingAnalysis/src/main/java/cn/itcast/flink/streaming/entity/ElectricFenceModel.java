package cn.itcast.flink.streaming.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 电子围栏规则计算模型 ElectricFenceModel
 *  该类包含 车辆的电子围栏数据 、 实时上报的车辆的经纬度 、 后续待分析的进栅栏、出栅栏的时间和告警数据
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ElectricFenceModel implements Comparable<ElectricFenceModel> {
    //车架号
    private String vin = "";
    //电子围栏结果表UUID
    private Long uuid = -999999L;
    //上次状态 0 里面 1 外面
    private int lastStatus = -999999;
    //当前状态 0  里面 1 外面
    private int nowStatus = -999999;
    //位置时间 yyyy-MM-dd HH:mm:ss
    private String gpsTime = "";
    //位置纬度--
    private Double lat = -999999D;
    //位置经度--
    private Double lng = -999999D;
    //电子围栏ID
    private int eleId = -999999;
    //电子围栏名称
    private String eleName = "";
    //中心点地址
    private String address = "";
    //中心点纬度
    private Double latitude;
    //中心点经度
    private Double longitude = -999999D;
    //电子围栏半径
    private Float radius = -999999F;
    //出围栏时间
    private String outEleTime = null;
    //进围栏时间
    private String inEleTime = null;
    //是否在mysql结果表中
    private Boolean inMysql = false;
    //状态报警 0：出围栏 1：进围栏
    private int statusAlarm = -999999;
    //报警信息
    private String statusAlarmMsg = "";
    //终端时间
    private String terminalTime = "";
    // 扩展字段 终端时间
    private Long terminalTimestamp = -999999L;
   
    @Override
    public int compareTo(ElectricFenceModel o) {
        if(this.getTerminalTimestamp() > o.getTerminalTimestamp()){
            return  1;
        }
        else if(this.getTerminalTimestamp() < o.getTerminalTimestamp()){
            return  -1;
        }else{
            return 0;
        }
    }
}