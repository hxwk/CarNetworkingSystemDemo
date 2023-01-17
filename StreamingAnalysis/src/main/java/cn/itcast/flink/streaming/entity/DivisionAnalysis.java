package cn.itcast.flink.streaming.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DivisionAnalysis {
    private String vin;
    private String name;
    private String analyzeValue1;
    private String analyzeValue2;
    private String analyzeValue3;
    private float analyzeType;
    private String terminalTime;
    // 省略getter、setter、construct、toString方法
}