package cn.tz.logAnalysis.hive;

import cn.tz.logAnalysis.datasource.ConnectionPool;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * @ClassName HttpStatusCount
 * @Description 利用Hive 统计日志数据中请求状态
 * @Author Administrator
 * @Version 1.0
 **/
public class HttpStatusCount {
     public static void main (String[] args) throws IOException {
         ConnectionPool connectionPool = ConnectionPool.getInstance();
         DruidPooledConnection conn = null;
         Statement statementMysql = null;
         Connection connection = null;
         Statement statement = null;
         String http_status = null;
         String count = null;
         Properties prop = new Properties();
         prop.load(new FileInputStream(new File("C:\\Users\\Administrator\\Log_Analysis\\src\\main\\resources\\hive_db.properties")));
         /**
          * 从Hive读取数据
          */
         try {
             Class.forName(prop.getProperty("driverName").toString());
             connection = DriverManager.getConnection(prop.getProperty("url").toString(),
                     prop.getProperty("user").toString(), prop.getProperty("password").toString());
             connection.setAutoCommit(false);
             statement = connection.createStatement();
             String hiveSql = "select http_status,count(1) as count from log_analysis1 group by http_status";
             ResultSet resultSet = statement.executeQuery(hiveSql);
             conn = connectionPool.getConnection();
             conn.setAutoCommit(false);
             statementMysql = conn.createStatement();
             while (resultSet.next()) {
                 http_status = resultSet.getString("http_status");
                 count = resultSet.getString("count");
                 String sql = "INSERT INTO http_status(status_code,count) VALUES('"+http_status+"','"+count+"')";
                 statementMysql.execute(sql);
                 conn.commit();
             }
         } catch (Exception e) {
             e.printStackTrace();
         }finally {
             if (statement != null && statementMysql != null) {
                 try {
                     statement.close();
                     statementMysql.close();
                 } catch (SQLException e) {
                     e.printStackTrace();
                 }
             }
             if (connection != null && conn != null){
                 try {
                     connection.close();
                     conn.close();
                 } catch (SQLException e) {
                     e.printStackTrace();
                 }
             }
         }
     }
}
