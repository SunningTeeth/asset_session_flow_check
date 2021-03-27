package com.lanysec.utils;

/**
 * @author daijb
 * @date 2021/3/27 14:09
 */
public class ConversionUtil {

    public static String toString(Object obj) {
        if (obj == null) {
            return "";
        }
        return obj.toString();
    }

    public static Integer toInteger(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Number) {
            return ((Number) obj).intValue();
        }
        String str = obj.toString();
        if (StringUtil.isEmpty(str)) {
            return null;
        }
        if (str.contains(".")) {
            return (int) Double.parseDouble(str);
        }
        return Integer.parseInt(str.trim());
    }

    public static int toInt(Object obj) {
        return toInt(obj, false);
    }

    public static int toInt(Object obj, boolean catchException) {
        int result = 0;
        try {
            Integer r0 = toInteger(obj);
            if (r0 != null) {
                result = r0;
            }
        } catch (Throwable t) {
            if (!catchException) {
                throw new RuntimeException(t);
            }
        }
        return result;
    }

    public static boolean toBoolean(Object obj) {
        return toBoolean(obj, false);
    }

    public static boolean toBoolean(Object obj, boolean defaultValue) {
        if (obj == null) {
            return defaultValue;
        }
        if (obj instanceof Boolean) {
            return ((Boolean) obj).booleanValue();
        } else if (obj.getClass() == boolean.class) {
            return ((Boolean) obj).booleanValue();
        }
        String str = obj.toString().trim();
        if (StringUtil.equalsIgnoreCase("true", str)
                || StringUtil.equalsIgnoreCase("yes", str)
                || StringUtil.equalsIgnoreCase("1", str)
        ) {
            return true;
        }
        if (StringUtil.equalsIgnoreCase("false", str)
                || StringUtil.equalsIgnoreCase("no", str)
                || StringUtil.equalsIgnoreCase("0", str)
        ) {
            return false;
        }
        return defaultValue;
    }

    public static double toDouble(Object obj) {
        double v = toDouble(obj, false);
        return (double) Math.round(v * 100) / 100;
    }

    public static double toDouble(Object obj, boolean catchException) {
        if (obj == null) {
            return 0;
        }
        if (obj instanceof Number) {
            return ((Number) obj).doubleValue();
        }
        try {
            return Double.parseDouble(obj.toString().trim());
        } catch (RuntimeException re) {
            if (catchException) {
                return Double.NaN;
            }
            throw re;
        }
    }

    public static int str2Minutes(String str) {
        if (StringUtil.isEmpty(str)) {
            return 5;
        }
        str = str.toLowerCase().trim();
        if (str.endsWith("m")) {
            str = str.substring(0, str.length() - 1);
            return toInt(str);
        } else if (str.endsWith("h")) {
            str = str.substring(0, str.length() - 1);
            return toInt(str) * 60;
        } else {
            //day
            str = str.substring(0, str.length() - 1);
            return toInt(str) * 60 * 24;
        }
    }
}
