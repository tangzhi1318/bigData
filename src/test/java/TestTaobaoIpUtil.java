import cn.tz.logAnalysis.util.TaobaoIP;
import cn.tz.logAnalysis.util.TaobaoIPResult;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @ClassName TestTaobaoIpUtil
 * @Description 测试淘宝接口
 * @Author Administrator
 * @Version 1.0
 **/
public class TestTaobaoIpUtil {
    public static void main (String[] args) throws Exception {
        TaobaoIP taobaoIP = new TaobaoIP("49.5.1.11");
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<TaobaoIPResult> result = executor.submit(taobaoIP);
        if (result.get().getCode() == 0){
            System.out.println("国家/地区：" + result.get().getCountry());
            System.out.println("省份：" + result.get().getRegion());
            System.out.println("城市：" + result.get().getCity());
            System.out.println("运营商：" + result.get().getIsp());
        }else {
            System.err.println("ip地址查询失败，请检查ip地址是否正确");
        }
    }
}
