package cn.itcast.flink.entity;

import com.oracle.webservices.internal.api.databinding.DatabindingMode;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Author itcast
 * Date 2022/6/4 11:04
 * Desc TODO
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CarModel {
    private int batteryAlarm;
    private int carMode;
    private double minVoltageBattery;
    private int chargeStatus;
    private String vin;

}
