package cn.tz.logAnalysis.ip;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName IpBean
 * @Description Ip实体类(实现hadoop序列化接口、根据Ip出现次数排序)
 * @Author Administrator
 * @Version 1.0
 **/
public class IpBean implements WritableComparable<IpBean> {
    String ip;
    long count;
    /**
     * 反序列化时，需要反射调用空参构造函数，所以要显示定义一个
     */
    public IpBean(){}

    public IpBean(String ip,long count) {
        super();
        this.ip = ip;
        this.count = count;
    }
    public void set(String ip,long count) {
        this.ip = ip;
        this.count = count;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    /**
     *自定义排序接口
     */
    @Override
    public int compareTo(IpBean o) {
        return this.getCount() > o.getCount()?-1:1;
    }
    /**
     * /**
     * 自定义序列化对象
     * 序列化
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
                dataOutput.writeUTF(ip);
                dataOutput.writeLong(count);
    }
    /**
     * 反序列化
     * 注意:hadoop序列化与反序列化顺序应一致
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
                ip = dataInput.readUTF();
                count = dataInput.readLong();
    }
    /**
     * 重写toString方法
     */
    @Override
    public String toString() {
        return  String.valueOf(count);
    }
}
