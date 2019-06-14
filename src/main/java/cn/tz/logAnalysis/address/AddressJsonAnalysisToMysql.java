package cn.tz.logAnalysis.address;

import cn.tz.logAnalysis.datasource.ConnectionPool;
import cn.tz.logAnalysis.util.BaiDuAPI;
import cn.tz.logAnalysis.util.baiduAPIResult;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.io.*;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName AddressJsonAnalysisToMysql
 * @Description 使用Hadoop MR程序分析好的Top Ip 调用百度地址API接口
 *              解析百度地址API接口返回的JSON数据并存入Mysql供Grafana前端展示
 * @Author Administrator
 * @Version 1.0
 **/
public class AddressJsonAnalysisToMysql {
    private static ConnectionPool connectionPool = ConnectionPool.getInstance();
    private static DruidPooledConnection conn = null;
    private static Statement statement = null;
       public static void main (String[] args) throws IOException, InterruptedException {
           BaiDuAPI baiDuAPI = new BaiDuAPI();
           baiduAPIResult baiduAPIResult = new baiduAPIResult();
           //BufferedReader是可以按行读取文件
           FileInputStream inputStream = new FileInputStream("C:\\Users\\Administrator\\Desktop\\address.txt");
           BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
           String line = null;
           while ((line = bufferedReader.readLine()) != null) {
               // 防止百度接口阻拦
               TimeUnit.SECONDS.sleep(1);
               String[] fileds = line.split("\t");
               baiduAPIResult = baiDuAPI.getLocation(fileds[0].toString());
               try {
                   conn = connectionPool.getConnection();
                   conn.setAutoCommit(false);
                   statement = conn.createStatement();
                   String province = baiduAPIResult.getProvince();
                   String city = baiduAPIResult.getCity();
                   String x = baiduAPIResult.getX();
                   String y = baiduAPIResult.getY();
                   String sql = "INSERT INTO address(provice,city,x,y) VALUES('"+province+"','"+city+"','"+x+"','"+y+"')";
                   statement.execute(sql);
                   conn.commit();
               } catch (SQLException e) {
                   e.printStackTrace();
               }finally {
                   if (statement != null) {
                       try {
                           statement.close();
                       } catch (SQLException e) {
                           e.printStackTrace();
                       }
                   }
                   if (conn != null) {
                       try {
                           conn.close();
                       } catch (SQLException e) {
                           e.printStackTrace();
                       }
                   }
               }
           }
       }
}
