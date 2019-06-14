import cn.tz.logAnalysis.datasource.ConnectionPool;
import com.alibaba.druid.pool.DruidPooledConnection;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @ClassName TestConnectionPool
 * @Description 测试数据库连接池
 * @Author Administrator
 * @Version 1.0
 **/
public class TestConnectionPool {
    public static void main (String[] args) {
        ConnectionPool connectionPool = ConnectionPool.getInstance();
        DruidPooledConnection conn = null;
        Statement statement = null;
        try {
            conn = connectionPool.getConnection();
            conn.setAutoCommit(false);
            statement = conn.createStatement();
            String sql = "SELECT * FROM city";
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                System.out.println(resultSet.getString("name"));
            }
        } catch (Exception e) {
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
