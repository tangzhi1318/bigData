import cn.tz.logAnalysis.util.BaiDuAPI;
import cn.tz.logAnalysis.util.baiduAPIResult;

/**
 * @ClassName TestBaiDuAPIUtil
 * @Description 测试百度接口
 * @Author Administrator
 * @Version 1.0
 **/
public class TestBaiDuAPIUtil {
    public static void main (String[] args) {
        BaiDuAPI baiDuAPI = new BaiDuAPI();
        baiduAPIResult result = baiDuAPI.getLocation("222.243.150.68");
        System.out.println(result.toString());
    }
}
