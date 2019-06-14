import org.apache.hadoop.hbase.util.Bytes;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.UUID;

/**
 * @ClassName TestRowkey
 * @Description 测试根据业务日期生成Rowkey
 * @Author Administrator
 * @Version 1.0
 **/
public class TestRowkey {
    public static byte[] getRowkey (String ip,String time) {
        // 1.获取uuid
        String preUuid2 = UUID.randomUUID().toString();
        // 2.去掉"-"符号
        String changUuid = preUuid2.substring(0,8)+preUuid2.substring(9,13)+preUuid2.substring(14,18)
                +preUuid2.substring(19,23)+preUuid2.substring(24);
        // 3.根据当前业务日期生成时间戳
        String[] split = time.split(":");
        String realTime = split[1] + ":" + split[2] + ":" + split[3];
        String date = "2013-05-30 "+realTime+"";
        long TimeMillis = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse(date, new ParsePosition(0)).getTime() / 1000;
        String rowkey = changUuid + "_" + ip + "_" + TimeMillis;
//        System.out.println(rowkey);
        return Bytes.toBytes(rowkey);
    }
    public static void main (String[] args) {
        byte[] rowkey = getRowkey("110.75.173.48", "[30/May/2013:23:59:58");
        System.out.println(rowkey.toString());
    }
}
