package cn.itcast.flink.streaming.util;

import cn.itcast.flink.streaming.entity.ItcastDataObj;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 将 json 字符串转换成 ItcastDataObj对象
 * 这个对象覆盖了 200+ 字段
 */
public class JsonParseUtil {
    private static final Logger logger = LoggerFactory.getLogger("JsonParseUtil");

    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * @desc:解析json成为ItcastDataObj对象
     * @param jsonString
     * @return 转换后的ItcastDataObj对象
     */
    public static ItcastDataObj parseJsonToObject(String jsonString) {
        ItcastDataObj itcastDataObj = new ItcastDataObj();
        try {
            HashMap vehicleMap = jsonToMap(jsonString);
            itcastDataObj.setGearDriveForce(convertIntType("gearDriveForce", vehicleMap));
            itcastDataObj.setBatteryConsistencyDifferenceAlarm(convertIntType("batteryConsistencyDifferenceAlarm", vehicleMap));
            itcastDataObj.setSoc(convertIntType("soc", vehicleMap));
            itcastDataObj.setSocJumpAlarm(convertIntType("socJumpAlarm", vehicleMap));
            itcastDataObj.setCaterpillaringFunction(convertIntType("caterpillaringFunction", vehicleMap));
            itcastDataObj.setSatNum(convertIntType("satNum", vehicleMap));
            itcastDataObj.setSocLowAlarm(convertIntType("socLowAlarm", vehicleMap));
            itcastDataObj.setChargingGunConnectionState(convertIntType("chargingGunConnectionState", vehicleMap));
            itcastDataObj.setMinTemperatureSubSystemNum(convertIntType("minTemperatureSubSystemNum", vehicleMap));
            itcastDataObj.setChargedElectronicLockStatus(convertIntType("chargedElectronicLockStatus", vehicleMap));
            itcastDataObj.setMaxVoltageBatteryNum(convertIntType("maxVoltageBatteryNum", vehicleMap));
            itcastDataObj.setTerminalTime(convertStringType("terminalTime", vehicleMap));
            itcastDataObj.setSingleBatteryOverVoltageAlarm(convertIntType("singleBatteryOverVoltageAlarm", vehicleMap));
            itcastDataObj.setOtherFaultCount(convertIntType("otherFaultCount", vehicleMap));
            itcastDataObj.setVehicleStorageDeviceOvervoltageAlarm(convertIntType("vehicleStorageDeviceOvervoltageAlarm", vehicleMap));
            itcastDataObj.setBrakeSystemAlarm(convertIntType("brakeSystemAlarm", vehicleMap));
            itcastDataObj.setServerTime(convertStringType("serverTime", vehicleMap));
            itcastDataObj.setVin(convertStringType("vin", vehicleMap).toUpperCase());
            itcastDataObj.setRechargeableStorageDevicesFaultCount(convertIntType("rechargeableStorageDevicesFaultCount", vehicleMap));
            itcastDataObj.setDriveMotorTemperatureAlarm(convertIntType("driveMotorTemperatureAlarm", vehicleMap));
            itcastDataObj.setGearBrakeForce(convertIntType("gearBrakeForce", vehicleMap));
            itcastDataObj.setDcdcStatusAlarm(convertIntType("dcdcStatusAlarm", vehicleMap));
            itcastDataObj.setLat(convertDoubleType("lat", vehicleMap));
            itcastDataObj.setDriveMotorFaultCodes(convertStringType("driveMotorFaultCodes", vehicleMap));
            itcastDataObj.setDeviceType(convertStringType("deviceType", vehicleMap));
            itcastDataObj.setVehicleSpeed(convertDoubleType("vehicleSpeed", vehicleMap));
            itcastDataObj.setLng(convertDoubleType("lng", vehicleMap));
            itcastDataObj.setChargingTimeExtensionReason(convertIntType("chargingTimeExtensionReason", vehicleMap));
            itcastDataObj.setGpsTime(convertStringType("gpsTime", vehicleMap));
            itcastDataObj.setEngineFaultCount(convertIntType("engineFaultCount", vehicleMap));
            itcastDataObj.setCarId(convertStringType("carId", vehicleMap));
            itcastDataObj.setCurrentElectricity(convertDoubleType("vehicleSpeed", vehicleMap));
            itcastDataObj.setSingleBatteryUnderVoltageAlarm(convertIntType("singleBatteryUnderVoltageAlarm", vehicleMap));
            itcastDataObj.setMaxVoltageBatterySubSystemNum(convertIntType("maxVoltageBatterySubSystemNum", vehicleMap));
            itcastDataObj.setMinTemperatureProbe(convertIntType("minTemperatureProbe", vehicleMap));
            itcastDataObj.setDriveMotorNum(convertIntType("driveMotorNum", vehicleMap));
            itcastDataObj.setTotalVoltage(convertDoubleType("totalVoltage", vehicleMap));
            itcastDataObj.setTemperatureDifferenceAlarm(convertIntType("temperatureDifferenceAlarm", vehicleMap));
            itcastDataObj.setMaxAlarmLevel(convertIntType("maxAlarmLevel", vehicleMap));
            itcastDataObj.setStatus(convertIntType("status", vehicleMap));
            itcastDataObj.setGeerPosition(convertIntType("geerPosition", vehicleMap));
            itcastDataObj.setAverageEnergyConsumption(convertDoubleType("averageEnergyConsumption", vehicleMap));
            itcastDataObj.setMinVoltageBattery(convertDoubleType("minVoltageBattery", vehicleMap));
            itcastDataObj.setGeerStatus(convertIntType("geerStatus", vehicleMap));
            itcastDataObj.setMinVoltageBatteryNum(convertIntType("minVoltageBatteryNum", vehicleMap));
            itcastDataObj.setValidGps(convertStringType("validGps", vehicleMap));
            itcastDataObj.setEngineFaultCodes(convertStringType("engineFaultCodes", vehicleMap));
            itcastDataObj.setMinTemperatureValue(convertDoubleType("minTemperatureValue", vehicleMap));
            itcastDataObj.setChargeStatus(convertIntType("chargeStatus", vehicleMap));
            itcastDataObj.setIgnitionTime(convertStringType("ignitionTime", vehicleMap));
            itcastDataObj.setTotalOdometer(convertDoubleType("totalOdometer", vehicleMap));
            itcastDataObj.setAlti(convertDoubleType("alti", vehicleMap));
            itcastDataObj.setSpeed(convertDoubleType("speed", vehicleMap));
            itcastDataObj.setSocHighAlarm(convertIntType("socHighAlarm", vehicleMap));
            itcastDataObj.setVehicleStorageDeviceUndervoltageAlarm(convertIntType("vehicleStorageDeviceUndervoltageAlarm", vehicleMap));
            itcastDataObj.setTotalCurrent(convertDoubleType("totalCurrent", vehicleMap));
            itcastDataObj.setBatteryAlarm(convertIntType("batteryAlarm", vehicleMap));
            itcastDataObj.setRechargeableStorageDeviceMismatchAlarm(convertIntType("rechargeableStorageDeviceMismatchAlarm", vehicleMap));
            itcastDataObj.setIsHistoryPoi(convertIntType("isHistoryPoi", vehicleMap));
            itcastDataObj.setVehiclePureDeviceTypeOvercharge(convertIntType("vehiclePureDeviceTypeOvercharge", vehicleMap));
            itcastDataObj.setMaxVoltageBattery(convertDoubleType("maxVoltageBattery", vehicleMap));
            itcastDataObj.setDcdcTemperatureAlarm(convertIntType("dcdcTemperatureAlarm", vehicleMap));
            itcastDataObj.setIsValidGps(convertStringType("isValidGps", vehicleMap));
            itcastDataObj.setLastUpdatedTime(convertStringType("lastUpdatedTime", vehicleMap));
            itcastDataObj.setDriveMotorControllerTemperatureAlarm(convertIntType("driveMotorControllerTemperatureAlarm", vehicleMap));
            itcastDataObj.setIgniteCumulativeMileage(convertDoubleType("igniteCumulativeMileage", vehicleMap));
            itcastDataObj.setDcStatus(convertIntType("dcStatus", vehicleMap));
            itcastDataObj.setRepay(convertStringType("repay", vehicleMap));
            itcastDataObj.setMaxTemperatureSubSystemNum(convertIntType("maxTemperatureSubSystemNum", vehicleMap));
            itcastDataObj.setMinVoltageBatterySubSystemNum(convertIntType("minVoltageBatterySubSystemNum", vehicleMap));
            itcastDataObj.setHeading(convertDoubleType("heading", vehicleMap));
            itcastDataObj.setTuid(convertStringType("tuid", vehicleMap));
            itcastDataObj.setEnergyRecoveryStatus(convertIntType("energyRecoveryStatus", vehicleMap));
            itcastDataObj.setFireStatus(convertIntType("fireStatus", vehicleMap));
            itcastDataObj.setTargetType(convertStringType("targetType", vehicleMap));
            itcastDataObj.setMaxTemperatureProbe(convertIntType("maxTemperatureProbe", vehicleMap));
            itcastDataObj.setRechargeableStorageDevicesFaultCodes(convertStringType("rechargeableStorageDevicesFaultCodes", vehicleMap));
            itcastDataObj.setCarMode(convertIntType("carMode", vehicleMap));
            itcastDataObj.setHighVoltageInterlockStateAlarm(convertIntType("highVoltageInterlockStateAlarm", vehicleMap));
            itcastDataObj.setInsulationAlarm(convertIntType("insulationAlarm", vehicleMap));
            itcastDataObj.setMileageInformation(convertIntType("mileageInformation", vehicleMap));
            itcastDataObj.setMaxTemperatureValue(convertDoubleType("maxTemperatureValue", vehicleMap));
            itcastDataObj.setOtherFaultCodes(convertStringType("otherFaultCodes", vehicleMap));
            itcastDataObj.setRemainPower(convertDoubleType("remainPower", vehicleMap));
            itcastDataObj.setInsulateResistance(convertIntType("insulateResistance", vehicleMap));
            itcastDataObj.setBatteryLowTemperatureHeater(convertIntType("batteryLowTemperatureHeater", vehicleMap));
            itcastDataObj.setFuelConsumption100km(convertStringType("fuelConsumption100km", vehicleMap));
            itcastDataObj.setFuelConsumption(convertStringType("fuelConsumption", vehicleMap));
            itcastDataObj.setEngineSpeed(convertStringType("engineSpeed", vehicleMap));
            itcastDataObj.setEngineStatus(convertStringType("engineStatus", vehicleMap));
            itcastDataObj.setTrunk(convertIntType("trunk", vehicleMap));
            itcastDataObj.setLowBeam(convertIntType("lowBeam", vehicleMap));
            itcastDataObj.setTriggerLatchOverheatProtect(convertStringType("triggerLatchOverheatProtect", vehicleMap));
            itcastDataObj.setTurnLndicatorRight(convertIntType("turnLndicatorRight", vehicleMap));
            itcastDataObj.setHighBeam(convertIntType("highBeam", vehicleMap));
            itcastDataObj.setTurnLndicatorLeft(convertIntType("turnLndicatorLeft", vehicleMap));
            itcastDataObj.setBcuSwVers(convertIntType("bcuSwVers", vehicleMap));
            itcastDataObj.setBcuHwVers(convertIntType("bcuHwVers", vehicleMap));
            itcastDataObj.setBcuOperMod(convertIntType("bcuOperMod", vehicleMap));
            itcastDataObj.setChrgEndReason(convertIntType("chrgEndReason", vehicleMap));
            itcastDataObj.setBCURegenEngDisp(convertStringType("BCURegenEngDisp", vehicleMap));
            itcastDataObj.setBCURegenCpDisp(convertIntType("BCURegenCpDisp", vehicleMap));
            itcastDataObj.setBcuChrgMod(convertIntType("bcuChrgMod", vehicleMap));
            itcastDataObj.setBatteryChargeStatus(convertIntType("batteryChargeStatus", vehicleMap));
            itcastDataObj.setBcuFltRnk(convertIntType("bcuFltRnk", vehicleMap));
            itcastDataObj.setBattPoleTOver(convertStringType("battPoleTOver", vehicleMap));
            itcastDataObj.setBcuSOH(convertDoubleType("bcuSOH", vehicleMap));
            itcastDataObj.setBattIntrHeatActive(convertIntType("battIntrHeatActive", vehicleMap));
            itcastDataObj.setBattIntrHeatReq(convertIntType("battIntrHeatReq", vehicleMap));
            itcastDataObj.setBCUBattTarT(convertStringType("BCUBattTarT", vehicleMap));
            itcastDataObj.setBattExtHeatReq(convertIntType("battExtHeatReq", vehicleMap));
            itcastDataObj.setBCUMaxChrgPwrLongT(convertStringType("BCUMaxChrgPwrLongT", vehicleMap));
            itcastDataObj.setBCUMaxDchaPwrLongT(convertStringType("BCUMaxDchaPwrLongT", vehicleMap));
            itcastDataObj.setBCUTotalRegenEngDisp(convertStringType("BCUTotalRegenEngDisp", vehicleMap));
            itcastDataObj.setBCUTotalRegenCpDisp(convertStringType("BCUTotalRegenCpDisp ", vehicleMap));
            itcastDataObj.setDcdcFltRnk(convertIntType("dcdcFltRnk", vehicleMap));
            itcastDataObj.setDcdcOutpCrrt(convertDoubleType("dcdcOutpCrrt", vehicleMap));
            itcastDataObj.setDcdcOutpU(convertDoubleType("dcdcOutpU", vehicleMap));
            itcastDataObj.setDcdcAvlOutpPwr(convertIntType("dcdcAvlOutpPwr", vehicleMap));
            itcastDataObj.setAbsActiveStatus(convertStringType("absActiveStatus", vehicleMap));
            itcastDataObj.setAbsStatus(convertStringType("absStatus", vehicleMap));
            itcastDataObj.setVcuBrkErr(convertStringType("VcuBrkErr", vehicleMap));
            itcastDataObj.setEPB_AchievedClampForce(convertStringType("EPB_AchievedClampForce", vehicleMap));
            itcastDataObj.setEpbSwitchPosition(convertStringType("epbSwitchPosition", vehicleMap));
            itcastDataObj.setEpbStatus(convertStringType("epbStatus", vehicleMap));
            itcastDataObj.setEspActiveStatus(convertStringType("espActiveStatus", vehicleMap));
            itcastDataObj.setEspFunctionStatus(convertStringType("espFunctionStatus", vehicleMap));
            itcastDataObj.setESP_TCSFailStatus(convertStringType("ESP_TCSFailStatus", vehicleMap));
            itcastDataObj.setHhcActive(convertStringType("hhcActive", vehicleMap));
            itcastDataObj.setTcsActive(convertStringType("tcsActive", vehicleMap));
            itcastDataObj.setEspMasterCylinderBrakePressure(convertStringType("espMasterCylinderBrakePressure", vehicleMap));
            itcastDataObj.setESP_MasterCylinderBrakePressureValid(convertStringType("ESP_MasterCylinderBrakePressureValid", vehicleMap));
            itcastDataObj.setEspTorqSensorStatus(convertStringType("espTorqSensorStatus", vehicleMap));
            itcastDataObj.setEPS_EPSFailed(convertStringType("EPS_EPSFailed", vehicleMap));
            itcastDataObj.setSasFailure(convertStringType("sasFailure", vehicleMap));
            itcastDataObj.setSasSteeringAngleSpeed(convertStringType("sasSteeringAngleSpeed", vehicleMap));
            itcastDataObj.setSasSteeringAngle(convertStringType("sasSteeringAngle", vehicleMap));
            itcastDataObj.setSasSteeringAngleValid(convertStringType("sasSteeringAngleValid", vehicleMap));
            itcastDataObj.setEspSteeringTorque(convertStringType("espSteeringTorque", vehicleMap));
            itcastDataObj.setAcReq(convertIntType("acReq", vehicleMap));
            itcastDataObj.setAcSystemFailure(convertIntType("acSystemFailure", vehicleMap));
            itcastDataObj.setPtcPwrAct(convertDoubleType("ptcPwrAct", vehicleMap));
            itcastDataObj.setPlasmaStatus(convertIntType("plasmaStatus", vehicleMap));
            itcastDataObj.setBattInTemperature(convertIntType("battInTemperature", vehicleMap));
            itcastDataObj.setBattWarmLoopSts(convertStringType("battWarmLoopSts", vehicleMap));
            itcastDataObj.setBattCoolngLoopSts(convertStringType("battCoolngLoopSts", vehicleMap));
            itcastDataObj.setBattCoolActv(convertStringType("battCoolActv", vehicleMap));
            itcastDataObj.setMotorOutTemperature(convertIntType("motorOutTemperature", vehicleMap));
            itcastDataObj.setPowerStatusFeedBack(convertStringType("powerStatusFeedBack", vehicleMap));
            itcastDataObj.setAC_RearDefrosterSwitch(convertIntType("AC_RearDefrosterSwitch", vehicleMap));
            itcastDataObj.setRearFoglamp(convertIntType("rearFoglamp", vehicleMap));
            itcastDataObj.setDriverDoorLock(convertIntType("driverDoorLock", vehicleMap));
            itcastDataObj.setAcDriverReqTemp(convertDoubleType("acDriverReqTemp", vehicleMap));
            itcastDataObj.setKeyAlarm(convertIntType("keyAlarm", vehicleMap));
            itcastDataObj.setAirCleanStsRemind(convertIntType("airCleanStsRemind", vehicleMap));
            itcastDataObj.setRecycleType(convertIntType("recycleType", vehicleMap));
            itcastDataObj.setStartControlsignal(convertStringType("startControlsignal", vehicleMap));
            itcastDataObj.setAirBagWarningLamp(convertIntType("airBagWarningLamp", vehicleMap));
            itcastDataObj.setFrontDefrosterSwitch(convertIntType("frontDefrosterSwitch", vehicleMap));
            itcastDataObj.setFrontBlowType(convertStringType("frontBlowType", vehicleMap));
            itcastDataObj.setFrontReqWindLevel(convertIntType("frontReqWindLevel", vehicleMap));
            itcastDataObj.setBcmFrontWiperStatus(convertStringType("bcmFrontWiperStatus", vehicleMap));
            itcastDataObj.setTmsPwrAct(convertStringType("tmsPwrAct", vehicleMap));
            itcastDataObj.setKeyUndetectedAlarmSign(convertIntType("keyUndetectedAlarmSign", vehicleMap));
            itcastDataObj.setPositionLamp(convertStringType("positionLamp", vehicleMap));
            itcastDataObj.setDriverReqTempModel(convertIntType("driverReqTempModel", vehicleMap));
            itcastDataObj.setTurnLightSwitchSts(convertIntType("turnLightSwitchSts", vehicleMap));
            itcastDataObj.setAutoHeadlightStatus(convertIntType("autoHeadlightStatus", vehicleMap));
            itcastDataObj.setDriverDoor(convertIntType("driverDoor", vehicleMap));
            itcastDataObj.setFrntIpuFltRnk(convertIntType("frntIpuFltRnk", vehicleMap));
            itcastDataObj.setFrontIpuSwVers(convertStringType("frontIpuSwVers", vehicleMap));
            itcastDataObj.setFrontIpuHwVers(convertIntType("frontIpuHwVers", vehicleMap));
            itcastDataObj.setFrntMotTqLongTermMax(convertIntType("frntMotTqLongTermMax", vehicleMap));
            itcastDataObj.setFrntMotTqLongTermMin(convertIntType("frntMotTqLongTermMin", vehicleMap));
            itcastDataObj.setCpvValue(convertIntType("cpvValue", vehicleMap));
            itcastDataObj.setObcChrgSts(convertIntType("obcChrgSts", vehicleMap));
            itcastDataObj.setObcFltRnk(convertStringType("obcFltRnk", vehicleMap));
            itcastDataObj.setObcChrgInpAcI(convertDoubleType("obcChrgInpAcI", vehicleMap));
            itcastDataObj.setObcChrgInpAcU(convertIntType("obcChrgInpAcU", vehicleMap));
            itcastDataObj.setObcChrgDcI(convertDoubleType("obcChrgDcI", vehicleMap));
            itcastDataObj.setObcChrgDcU(convertDoubleType("obcChrgDcU", vehicleMap));
            itcastDataObj.setObcTemperature(convertIntType("obcTemperature", vehicleMap));
            itcastDataObj.setObcMaxChrgOutpPwrAvl(convertIntType("obcMaxChrgOutpPwrAvl", vehicleMap));
            itcastDataObj.setPassengerBuckleSwitch(convertIntType("passengerBuckleSwitch", vehicleMap));
            itcastDataObj.setCrashlfo(convertStringType("crashlfo", vehicleMap));
            itcastDataObj.setDriverBuckleSwitch(convertIntType("driverBuckleSwitch", vehicleMap));
            itcastDataObj.setEngineStartHibit(convertStringType("engineStartHibit", vehicleMap));
            itcastDataObj.setLockCommand(convertStringType("lockCommand", vehicleMap));
            itcastDataObj.setSearchCarReq(convertStringType("searchCarReq", vehicleMap));
            itcastDataObj.setAcTempValueReq(convertStringType("acTempValueReq", vehicleMap));
            itcastDataObj.setVcuErrAmnt(convertStringType("vcuErrAmnt", vehicleMap));
            itcastDataObj.setVcuSwVers(convertIntType("vcuSwVers", vehicleMap));
            itcastDataObj.setVcuHwVers(convertIntType("vcuHwVers", vehicleMap));
            itcastDataObj.setLowSpdWarnStatus(convertStringType("lowSpdWarnStatus", vehicleMap));
            itcastDataObj.setLowBattChrgRqe(convertIntType("lowBattChrgRqe", vehicleMap));
            itcastDataObj.setLowBattChrgSts(convertStringType("lowBattChrgSts", vehicleMap));
            itcastDataObj.setLowBattU(convertDoubleType("lowBattU", vehicleMap));
            itcastDataObj.setHandlebrakeStatus(convertIntType("handlebrakeStatus", vehicleMap));
            itcastDataObj.setShiftPositionValid(convertStringType("shiftPositionValid", vehicleMap));
            itcastDataObj.setAccPedalValid(convertStringType("accPedalValid", vehicleMap));
            itcastDataObj.setDriveMode(convertIntType("driveMode", vehicleMap));
            itcastDataObj.setDriveModeButtonStatus(convertIntType("driveModeButtonStatus", vehicleMap));
            itcastDataObj.setVCUSRSCrashOutpSts(convertIntType("VCUSRSCrashOutpSts", vehicleMap));
            itcastDataObj.setTextDispEna(convertIntType("textDispEna", vehicleMap));
            itcastDataObj.setCrsCtrlStatus(convertIntType("crsCtrlStatus", vehicleMap));
            itcastDataObj.setCrsTarSpd(convertIntType("crsTarSpd", vehicleMap));
            itcastDataObj.setCrsTextDisp(convertIntType("crsTextDisp",vehicleMap ));
            itcastDataObj.setKeyOn(convertIntType("keyOn", vehicleMap));
            itcastDataObj.setVehPwrlim(convertIntType("vehPwrlim", vehicleMap));
            itcastDataObj.setVehCfgInfo(convertStringType("vehCfgInfo", vehicleMap));
            itcastDataObj.setVacBrkPRmu(convertIntType("vacBrkPRmu", vehicleMap));

            /* ------------------------------------------nevChargeSystemVoltageDtoList 可充电储能子系统电压信息列表-------------------------------------- */
            List<Map<String, Object>> nevChargeSystemVoltageDtoList = jsonToList(vehicleMap.getOrDefault("nevChargeSystemVoltageDtoList", new ArrayList<Object>()).toString());
            if (!nevChargeSystemVoltageDtoList.isEmpty()) {
                // 只取list中的第一个map集合,集合中的第一条数据为有效数据
                Map<String, Object> nevChargeSystemVoltageDtoMap = nevChargeSystemVoltageDtoList.get(0);
                itcastDataObj.setCurrentBatteryStartNum(convertIntType("currentBatteryStartNum",nevChargeSystemVoltageDtoMap));
                itcastDataObj.setBatteryVoltage(convertJoinStringType("batteryVoltage", nevChargeSystemVoltageDtoMap));
                itcastDataObj.setChargeSystemVoltage(convertDoubleType("chargeSystemVoltage",nevChargeSystemVoltageDtoMap));
                itcastDataObj.setCurrentBatteryCount(convertIntType("currentBatteryCount", nevChargeSystemVoltageDtoMap));
                itcastDataObj.setBatteryCount(convertIntType("batteryCount", nevChargeSystemVoltageDtoMap));
                itcastDataObj.setChildSystemNum(convertIntType("childSystemNum", nevChargeSystemVoltageDtoMap));
                itcastDataObj.setChargeSystemCurrent(convertDoubleType("chargeSystemCurrent", nevChargeSystemVoltageDtoMap));
            }
            /* -------------------------------------------------------------------------------------------------------------- */

            /* ----------------------------------------------driveMotorData-------------------------------------------------- */
            List<Map<String, Object>> driveMotorData = jsonToList(vehicleMap.getOrDefault("driveMotorData", new ArrayList()).toString());                                    //驱动电机数据
            if (!driveMotorData.isEmpty()) {
                Map<String, Object> driveMotorMap = driveMotorData.get(0);
                itcastDataObj.setControllerInputVoltage(convertDoubleType("controllerInputVoltage", driveMotorMap));
                itcastDataObj.setControllerTemperature(convertDoubleType("controllerTemperature", driveMotorMap));
                itcastDataObj.setRevolutionSpeed(convertDoubleType("revolutionSpeed", driveMotorMap));
                itcastDataObj.setNum(convertIntType("num", driveMotorMap));
                itcastDataObj.setControllerDcBusCurrent(convertDoubleType("controllerDcBusCurrent", driveMotorMap));
                itcastDataObj.setTemperature(convertDoubleType("temperature", driveMotorMap));
                itcastDataObj.setTorque(convertDoubleType("torque", driveMotorMap));
                itcastDataObj.setState(convertIntType("state", driveMotorMap));
            }
            /* -------------------------------------------------------------------------------------------------------------- */

            /* -----------------------------------------nevChargeSystemTemperatureDtoList------------------------------------ */
            List<Map<String, Object>> nevChargeSystemTemperatureDtoList = jsonToList(vehicleMap.getOrDefault("nevChargeSystemTemperatureDtoList", new ArrayList()).toString());
            if (!nevChargeSystemTemperatureDtoList.isEmpty()) {
                Map<String, Object> nevChargeSystemTemperatureMap = nevChargeSystemTemperatureDtoList.get(0);
                itcastDataObj.setProbeTemperatures(convertJoinStringType("probeTemperatures", nevChargeSystemTemperatureMap));
                itcastDataObj.setChargeTemperatureProbeNum(convertIntType("chargeTemperatureProbeNum", nevChargeSystemTemperatureMap));
            }
            /* -------------------------------------------------------------------------------------------------------------- */

            /* --------------------------------------------ecuErrCodeDataList------------------------------------------------ */
            Map<String, Object> xcuerrinfoMap = jsonToMap(vehicleMap.getOrDefault("xcuerrinfo", new HashMap<String, Object>()).toString());
            if (!xcuerrinfoMap.isEmpty()) {
                List<Map<String, Object>> ecuErrCodeDataList = jsonToList(xcuerrinfoMap.getOrDefault("ecuErrCodeDataList", new ArrayList()).toString()) ;
                if (ecuErrCodeDataList.size() > 4) {
                    itcastDataObj.setVcuFaultCode(convertJoinStringType("errCodes", ecuErrCodeDataList.get(0)));
                    itcastDataObj.setBcuFaultCodes(convertJoinStringType("errCodes", ecuErrCodeDataList.get(1)));
                    itcastDataObj.setDcdcFaultCode(convertJoinStringType("errCodes", ecuErrCodeDataList.get(2)));
                    itcastDataObj.setIpuFaultCodes(convertJoinStringType("errCodes", ecuErrCodeDataList.get(3)));
                    itcastDataObj.setObcFaultCode(convertJoinStringType("errCodes", ecuErrCodeDataList.get(4)));
                }
            }
            /* -------------------------------------------------------------------------------------------------------------- */
			
            // carStatus不在有效范围，设置值为255
            if (convertStringType("carStatus", vehicleMap).length() > 3) {
                itcastDataObj.setCarStatus(255);
            } else {
                itcastDataObj.setCarStatus(convertIntType("carStatus", vehicleMap));
            }
            // terminalTime字段不为空，设置标记时间为terminalTime时间
            if(!itcastDataObj.getTerminalTime().isEmpty()){
                itcastDataObj.setTerminalTimeStamp(sdf.parse(itcastDataObj.getTerminalTime()).getTime());
            }
        } catch (Exception e){
            itcastDataObj.setErrorData(jsonString);
            logger.error("json 数据格式错误...", e);
        }
        // 如果没有VIN号和终端时间，则为无效数据
        if(itcastDataObj.getVin().isEmpty() || itcastDataObj.getTerminalTime().isEmpty() || itcastDataObj.getTerminalTimeStamp() < 1){
            if(itcastDataObj.getVin().isEmpty()){
                logger.error("vin.isEmpty");
            }
            if(itcastDataObj.getTerminalTime().isEmpty()){
                logger.error("terminalTime.isEmpty");
            }
            itcastDataObj.setErrorData(jsonString);
        }
        return itcastDataObj;
    }

    /**
     * @desc:将Json对象转换成Map
     * @param jsonString
     * @return Map对象
     * @throws JSONException
     */
    public static HashMap jsonToMap(String jsonString) throws JSONException {
        JSONObject jsonObject = new JSONObject(jsonString);
        HashMap result = new HashMap();
        Iterator iterator = jsonObject.keys();
        String key = null;
        Object value = null;
        while (iterator.hasNext()) {
            key = (String) iterator.next();
            value = jsonObject.get(key);
            result.put(key, value);
        }
        return result;
    }

    /**
     * @desc:将数组转换为List，数组内部的json字符串转换为map
     * @param jsonString
     * @return List<Map<String, Object>>
     * @throws JSONException
     */
    public static List<Map<String, Object>> jsonToList(String jsonString) throws JSONException {
        List<Map<String, Object>> resultList = new ArrayList();
        JSONArray jsonArray = new JSONArray(jsonString);
        for (int i = 0; i < jsonArray.length(); i++) {
            HashMap map =jsonToMap(jsonArray.get(i).toString());
            resultList.add(map);
        }
        return resultList;
    }

    /**
     * 提取类型重复转换代码
     * @param fieldName
     * @param map
     * @return 对应数据类型的值
     */
    private static int convertIntType(String fieldName, Map<String, Object> map) {
        return Integer.parseInt(map.getOrDefault(fieldName, -999999).toString());
    }
    private static String convertStringType(String fieldName, Map<String, Object> map) {
        return map.getOrDefault(fieldName, "").toString();
    }
    private static double convertDoubleType(String fieldName, Map<String, Object> map) {
        return Double.parseDouble(map.getOrDefault(fieldName, -999999).toString());
    }
    private static String convertJoinStringType(String fieldName, Map<String, Object> map) {
        return String.join("~", convertStringToArray(map.getOrDefault(fieldName, new ArrayList()).toString()));
    }

    /**
     * 解决类型转换异常：string转数组,字符串以","分割
     * @param str
     * @return list
     */
    private static List convertStringToArray(String str) {
        return Arrays.asList(str.split(","));
    }
}