package cn.tz.logAnalysis.hive;

import cn.tz.logAnalysis.datasource.ConnectionPool;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/**
 * @ClassName HttpMethodCount
 * @Description 利用Hive 统计日志数据中请求方式
 * @Author Administrator
 * @Version 1.0
 **/
public class HttpMethodCount {
    public static void main (String[] args) throws IOException {
        ConnectionPool connectionPool = ConnectionPool.getInstance();
        DruidPooledConnection conn = null;
        Statement statementMysql = null;
        Connection connection = null;
        Statement statement = null;
        String http_method = null;
        String count = null;
        String real_http_method = null;
        Properties prop = new Properties();
        prop.load(new FileInputStream(new File("C:\\Users\\Administrator\\Log_Analysis\\src\\main\\resources\\hive_db.properties")));
        /**
         * 从Hive读取数据
         */
        try {
            Class.forName(prop.getProperty("driverName").toString());
            connection = DriverManager.getConnection(prop.getProperty("url").toString(), prop.getProperty("user").toString(), prop.getProperty("password").toString());
            connection.setAutoCommit(false);
            statement = connection.createStatement();
            String hiveSql = "select http_method,count(1) as count from log_analysis1 group by  http_method";
            ResultSet resultSet = statement.executeQuery(hiveSql);
            conn = connectionPool.getConnection();
            conn.setAutoCommit(false);
            statementMysql = conn.createStatement();
            while (resultSet.next()) {
                http_method = resultSet.getString("http_method");
                count = resultSet.getString("count");
                real_http_method = http_method.replace("\"", "");
                /**
                 * 存入Mysql数据库
                 */
                String sql = "INSERT INTO http_method(method_name,count) VALUES('"+real_http_method+"','"+count+"')";
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
