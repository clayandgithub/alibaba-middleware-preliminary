package com.alibaba.middleware.race.util;

public class DoubleUtil {
    public static double roundedTo2Digit(double originalValue) {
        return Double.parseDouble(String.format("%.2f", originalValue));
    }
}
