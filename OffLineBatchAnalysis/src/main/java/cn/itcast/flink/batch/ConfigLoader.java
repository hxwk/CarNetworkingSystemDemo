package cn.itcast.flink.batch;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Author itcast
 * Date 2022/6/4 17:06
 * Desc 将 conf.properties 配置文件中的所有属性根据指定的key读取出来
 */
public class ConfigLoader {
    //静态代码块，在类加载的时候就将 conf.properties 文件中的 key=value 加载内存中
    static Properties props = new Properties();

    static {
        InputStream is = ConfigLoader.class.getClassLoader()
                .getResourceAsStream("conf.properties");

        try {
            props.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取指定key的value值（String）
    public static String get(String key) {
        return props.getProperty(key);
    }

    //获取指定的key的value值（Integer）
    public static Integer getInt(String key) {
        String value = props.getProperty(key);
        int num = -999999;
        if (StringUtils.isNotBlank(value)) {
            num = Integer.parseInt(value);
        }
        return num;
    }
}
