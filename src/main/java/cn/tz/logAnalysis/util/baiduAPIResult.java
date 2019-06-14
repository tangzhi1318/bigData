package cn.tz.logAnalysis.util;

/**
 * @ClassName baiduAPIResult
 * @Description 解析JSON实体类
 * @Author Administrator
 * @Version 1.0
 **/
public class baiduAPIResult {
       private String city;
       private String province;
       private String x;
       private String y;

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getX() {
        return x;
    }

    public void setX(String x) {
        this.x = x;
    }

    public String getY() {
        return y;
    }

    public void setY(String y) {
        this.y = y;
    }

    @Override
    public String toString() {
        return "省份："+province + " " + "城市：" + city + " " + "经纬度：" + "(" + x + "," + y +")";
    }
}
