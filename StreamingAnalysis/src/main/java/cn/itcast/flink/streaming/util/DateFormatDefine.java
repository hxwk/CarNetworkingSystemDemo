package cn.itcast.flink.streaming.util;

/**
 * 枚举：可穷举的常量类型
 * 枚举是一种特殊的 class ，存储的固定的常量类型
 * 可以定义变量及构造器
 */
public enum DateFormatDefine {

    DATE_TIME_FORMAT("yyyy-MM-dd HH:mm:ss"),
    DATE_FORMAT("yyyyMMdd"),
    DATE2_FORMAT("yyyy-MM-dd");

    //定义变量
    private String format;

    DateFormatDefine(String _format) {
        this.format = _format;
    }

    /**
     * 设置 format 格式
     * @return
     */
    public String getFormat() {
        return format;
    }
}
