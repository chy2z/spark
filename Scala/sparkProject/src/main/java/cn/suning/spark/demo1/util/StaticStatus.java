package cn.suning.spark.demo1.util;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class StaticStatus
{
    public static final List<String> CATALOG_LIST = new ArrayList();
    public static final String DEL_STORE_FLAG = "D";
    public static final String SWL_FLAG = "7";
    public static final String STR_AT_MART = "@@";
    public static final String STR_POUND = "#";
    public static final String STR_SOLR = "solr_";
    public static final String STR_DP = "&_1_&";
    public static final String SN_VENDORTCODE = "SN_001";
    public static final String NATION_CITY = "A001";
    public static final String NOT_SALE_NATION_CITY = "NA001";
    public static final String SUNING_TYPE = "s";
    public static final String OTC_TYPE = "otc";
    public static final String PHONE_ZEROGROUP = "1";
    public static final String PHONE_CONTRACTGROUP = "2";
    public static final String PHONE_3GCARD = "3";
    public static final String PHONE_SELECT_NUM_NET = "4";
    public static final String SN_PHONE_ZEROGROUP = "5";
    public static final String PHONE_OLD_TYPE = "8";
    public static final String SN_PHONE_CONTRACTGROUP = "6";
    public static final String SN_PHONE_NETWORK = "8";
    public static final String STR_TWO_AT_MART = "@@";
    public static final String ANDROID_PLANTFORM = "9203";
    public static final String WP8_PLANTFORM = "9201";
    public static final String IOS_PLATFORM = "9202";
    public static final String CATALOG_BOOK = "22001";
    public static final String CATALOG_BOOK_NEW = "25004";
    public static final String CATALOG_ELEC = "10051";
    public static final String CATALOG_BEAUTY = "14656";
    public static final String CATALOG_BABY = "14655";
    public static final String CATALOG_EBOOK = "14155";
    public static final int STA_PUBLISH = 1;
    public static final int STA_UNPUBLISH = 0;
    public static final int STA_INC = 1;
    public static final int STA_ALL = 0;
    public static final int VENDORTYPE_SN = 1;
    public static final int VENDORTYPE_CS = 2;
    public static final int VENDORTYPE_SWL = 4;
    public static String serverTime = null;
    public static final String TIME_PATTERN_YMD = "yyyy-MM-dd";
    public static final String TIME_PATTERN_YYYYMMDD = "yyyy-MM-dd";
    public static final String TIME_PATTERN_YMDHMS = "yyyy-MM-dd HH:mm:ss";
    public static final String TIME_PATTERN_YMDHMS_P = "yyyy/MM/dd HH:mm:ss";
    public static final String TIME_PATTERNYMDHMS = "yyyyMMddHHmmss";
    public static final String TIME_PATTERNYMDHMSS = "yyyyMMddHHmmssSS";
    public static final String TIME_PATTERNYMDHMSSS = "yyyyMMddHHmmssSSS";
    public static final String TIME_PATTERNSSS = "ssSSS";
    public static final String TIME_PATTERNYMD = "yyyyMMdd";
    public static final String TIME_PATTERNHMSS = "HHmmssSS";
    public static final String FILE_EXTENSION_CSV = "csv";
    public static final String FILE_EXTENSION_CCSV = ".csv";
    public static final String RESERVATION_TYPE_S = "1";
    public static final String RESERVATION_TYPE_R = "0";
    public static final String SIGN_COMMA = ",";
    public static final String SIGN_SEMICOLON = ";";
    public static final String SIGN_AT = "@";
    public static final String CHART_CODE = "UTF-8";
    public static final String SIGN_ATAT = "@A@";
    public static final String SIGN_TILDE = "~_~";
    public static final String SIGN_NPC = "NPC";
    public static final String CATALOG_NO_RESERVATION = "NO";
    public static final String CATALOG_RESERVATION = "O";
    public static final String CATALOG_NO_CONTRACT = "NH";
    public static final String CATALOG_CONTRACT = "H";
    public static final String CATALOG_CONTRACT_OLD = "OH";
    public static final String SIGN_COLON = ":";
    public static final String SIGN_ZERO = "0";
    public static final String SIGN_UP_LINE = "_";
    public static final String SIGN_LINE = "-";
    public static final String SIGN_SLANT = "/";
    public static final String SIGN_VER_LINE = "|";
    public static final String SIGN_SPOT = ".";
    public static final String SIGN_BT = "BT_";
    public static final char[] DB2_HEX_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
    public static final String PARTNUMBER_ZERO_PRE = "000000000";
    public static final String CITY_ZERO_PRE = "00000";
    public static final String SWITCH_OFF = "off";
    public static final String SWITCH_ON = "on";
    public static final String DEFAULT_COLLECT_TIME = "1970-01-01 08:00:00.0";
    public static Date PRODUCT_REC_START_TIME = new Date();

    public static long PRODUCT_ALL_COUNT = 0L;

    public static long PRODUCT_VALID_COUNT = 0L;
    public static int SHARD_INDEX;
    public static int SHARD_COUNT;
    public static final int NATION_SPE_CITY = -1;
    public static final String NATION_MDM_CITY = "1000999";
    public static final String NATION_CITY_900 = "9000000";
    public static final int FINDSOURCE_IS_TRUE = 1;
    public static final int FINDSOURCE_IS_FALSE = 0;
    public static final String REGEXP = "[+-[&]||!(){}\\[\\]^\"~*?:/]";
    public static final int yearMonthDay = 10;
    public static final int yearMonth = 7;
    public static final int year = 4;
    public static final int yearMonthDay2 = 8;
    public static final int yearMonthDay3 = 9;
    public static final int INC_MAINB_SIZE = 0;
    public static final String book_attrId1 = "005423";
    public static final String book_attrId2 = "010070";
    public static final String book_attrId3 = "010086";
    public static final String book_attrId4 = "010099";
    public static final String book_attrId5 = "005004";
    public static final String book_attrId6 = "005754";
    public static final String book_attrId7 = "010076";
    public static final String SN_VERSION = "0000000000";
    public static final int HEXADECIMAL = 2147483647;

    static
    {
        CATALOG_LIST.add("10051");
        CATALOG_LIST.add("14655");
        CATALOG_LIST.add("14656");
        CATALOG_LIST.add("22001");
        CATALOG_LIST.add("25003");
        CATALOG_LIST.add("25004");
    }
}