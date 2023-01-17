package cn.itcast.flink.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CarModelPlus {
    private int batteryAlarm;
    private int carMode;
    private double minVoltageBattery;
    private int chargeStatus;
    private String vin;
    private List<Integer> probeTemperatures;
    private int chargeTemperatureProbeNum;
    private int childSystemNum;
}
