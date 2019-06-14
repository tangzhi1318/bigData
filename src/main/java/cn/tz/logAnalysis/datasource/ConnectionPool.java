package cn.tz.logAnalysis.datasource;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.alibaba.druid.pool.DruidPooledConnection;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @ClassName ConnectionPool
 * @Description 数据库连接池
 * @Author Administrator
 * @Version 1.0
 **/
public class ConnectionPool {
    static Logger log = Logger.getLogger(ConnectionPool.class);
    private static ConnectionPool dbConnectionPool = null;
    private static DruidDataSource druidDataSource = null;
    private static Properties prop = null;
    /**
     * 读取配置文件获取druidDataSource
     */
    static {
        try {
            prop  = new Properties();
            prop.load(new FileInputStream(new File("C:\\Users\\Administrator\\Log_Analysis\\src\\main\\resources\\db.properties")));
            //DruidDataSource工厂模式
            druidDataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(prop);
        } catch (Exception e) {
            log.error("获取配置失败");
        }
    }
    /**
     * 数据库连接池单例
     */
    public static synchronized ConnectionPool getInstance() {
        if (dbConnectionPool == null) {
            dbConnectionPool = new ConnectionPool();
        }
        return dbConnectionPool;
    }
    /**
     * 获取连接对象
     */
    public DruidPooledConnection getConnection() throws SQLException {
        return druidDataSource.getConnection();
    }
}
