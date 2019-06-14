package cn.tz.logAnalysis.util;

import org.nutz.http.Http;
import org.nutz.http.Response;
import org.nutz.json.Json;

import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @ClassName TaobaoIP
 * @Description 淘宝IP地址服务工具
 * @Author Administrator
 * @Version 1.0
 **/
public class TaobaoIP implements Callable<TaobaoIPResult>{
    private String ip;
    public TaobaoIP(String ip) {
        this.ip = ip;
    }
    @Override
    public TaobaoIPResult call() throws Exception {
        Response response = Http.get("http://ip.taobao.com/service/getIpInfo.php?ip=" + ip);
        TaobaoIPResult result = new TaobaoIPResult();
        if (ip!=null && response.getStatus() == 200){
            try {
                String content = response.getContent();
                Map<String, Object> contentMap = (Map<String, Object>) Json.fromJson(content);
                if ((Integer)(contentMap.get("code")) == 0){
                    Map<String, Object>dataMap = (Map<String, Object>) contentMap.get("data");
                    result.setCountry((String) dataMap.get("country"));
                    result.setRegion((String) dataMap.get("region"));
                    result.setCity((String) dataMap.get("city"));
                    result.setCounty((String) dataMap.get("county"));
                    result.setIsp((String) dataMap.get("isp"));
                    result.setArea((String) dataMap.get("area"));
                    result.setIp((String) dataMap.get("ip"));
                    result.setCode(0);
                    Thread.sleep(3000);
                    return result;
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        result.setCode(-1);
        result.setCountry("XX");
        result.setRegion("XX");
        result.setCity("XX");
        result.setCounty("XX");
        result.setIsp("XX");
        result.setArea("XX");
        result.setIp(ip);
        Thread.sleep(3000);
        return result;
    }
    /**
     *淘宝IP地址服务工具
     *依赖于nutz.jar(https://nutz.cn/nutzdw/)
     * @param ip
     * @return TaobaoIPResult
     */

}
