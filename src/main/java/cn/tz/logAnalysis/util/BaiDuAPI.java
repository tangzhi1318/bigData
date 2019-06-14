package cn.tz.logAnalysis.util;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

/**
 * @ClassName BaiDuAPI
 * @Description 百度地址服务工具
 * @Author Administrator
 * @Version 1.0
 **/
public class BaiDuAPI {
       private InputStreamReader isr = null;
       public  baiduAPIResult getLocation (String ip) {
              // 经度
              String x = null;
              // 纬度
              String y = null;
              // 城市名
              String city = null;
              // 省份
              String province = null;
              baiduAPIResult baiduAPIResult = new baiduAPIResult();
              if (ip != null) {
                     String url = "http://api.map.baidu.com/location/ip?ak=32f38c9491f2da9eb61106aaab1e9739&ip="+ip+"&coor=bd09ll";
                     String json = loadJSON(url.toString());
                     JSONObject object = JSONObject.parseObject(json);
                     // 判断状态码
                     if ("0".equals(object.getString("status"))) {
                            // 返回状态成功
                            city = object.getJSONObject("content").getJSONObject("address_detail").getString("city");
                            x = object.getJSONObject("content").getJSONObject("point").getString("x");
                            y = object.getJSONObject("content").getJSONObject("point").getString("y");
                            province = object.getJSONObject("content").getJSONObject("address_detail").getString("province");
                            baiduAPIResult.setCity(city);
                            baiduAPIResult.setX(x);
                            baiduAPIResult.setY(y);
                            baiduAPIResult.setProvince(province);
                     }else {
                            // 返回状态失败
                            baiduAPIResult.setCity("XX");
                            baiduAPIResult.setX("XX");
                            baiduAPIResult.setY("XX");
                            baiduAPIResult.setProvince("XX");
                     }
              }
              return baiduAPIResult;
       }
       public  String loadJSON(String url) {
              String json = null;
              try {
                     URL mapAPI = new URL(url);
                     URLConnection connection = mapAPI.openConnection();
                     isr = new InputStreamReader(connection.getInputStream(), "utf-8");
                     json= IOUtils.toString(isr);
              } catch (Exception e) {
                     e.printStackTrace();
              } finally {
                     try {
                            if (isr != null) {
                                   isr.close();
                            }
                     } catch (IOException e) {
                            e.printStackTrace();
                     }
              }
              return json;
       }
}
