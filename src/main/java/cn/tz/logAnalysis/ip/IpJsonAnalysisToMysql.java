package cn.tz.logAnalysis.ip;

import cn.tz.logAnalysis.datasource.ConnectionPool;
import cn.tz.logAnalysis.util.TaobaoIP;
import cn.tz.logAnalysis.util.TaobaoIPResult;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.*;

/**
 * @ClassName IpJsonAnalysisToMysql
 * @Description 使用Hadoop MR程序分析好的Top Ip 调用淘宝Ip接口
 *              解析淘宝Ip接口返回的JSON数据并存入Mysql供Grafana前端展示
 * @Author Administrator
 * @Version 1.0
 **/
public class IpJsonAnalysisToMysql {
           private static ConnectionPool connectionPool = ConnectionPool.getInstance();
           private static DruidPooledConnection conn = null;
           private static Statement statement = null;
           /**
            * 手动创建线程池
            */
           private static ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("MyThread-pool-%d").build();
           private static ExecutorService executor = new ThreadPoolExecutor(5, 200, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
           public static void main (String[] args) throws Exception {
               //BufferedReader是可以按行读取文件
               FileInputStream inputStream = new FileInputStream("C:\\Users\\Administrator\\Desktop\\ip.txt");
               BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
               String line = null;
               while ((line = bufferedReader.readLine()) != null) {
                   TimeUnit.SECONDS.sleep(2);
                   String[] fileds = line.split("\t");
                   TaobaoIP taobaoIP = new TaobaoIP(fileds[0].toString());
                   Future<TaobaoIPResult> result = executor.submit(taobaoIP);
                   try {
                       conn = connectionPool.getConnection();
                       //取消自动提交
                       conn.setAutoCommit(false);
                       statement = conn.createStatement();
                       String region = result.get().getRegion();
                       String city = result.get().getCity();
                       String isp = result.get().getIsp();
                       String sql = "INSERT INTO ip(city,provice,isp,count) VALUES('" + city + "','" + region + "','" + isp + "','"+fileds[1]+"')";
                       statement.execute(sql);
                       conn.commit();
                   }catch (Exception e) {
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
