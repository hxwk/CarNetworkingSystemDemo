package cn.itcast.flink.streaming;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Author itcast
 * Date 2022/6/9 14:53
 * Desc TODO
 */
public class DateTimeFormatterTest {
    public static void main(String[] args) {
        LocalDate date = LocalDate.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy/MM/dd");
        String text = date.format(formatter);
        // sdf.format(日期)
        LocalDate parsedDate = LocalDate.parse(text, formatter);
        System.out.println(text);

    }
}
