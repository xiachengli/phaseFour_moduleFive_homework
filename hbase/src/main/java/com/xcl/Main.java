package com.xcl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class Main {

    Configuration conf = null;
    Connection conn = null;

    @Before
    public void init() throws IOException {
        //获取一个配置文件对象
        conf = HBaseConfiguration.create();

        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        //通过conf获取到hbase集群的连接
        conn = ConnectionFactory.createConnection(conf);
    }


    //删除一条数据
    @Test
    public void deleteData() throws IOException {
        //需要获取一个table对象
        Table table = conn.getTable(TableName.valueOf("relationship"));

        //准备delete对象
        Delete delete = new Delete(Bytes.toBytes("101"));
        delete.addColumn(Bytes.toBytes("friends"), Bytes.toBytes("102"));

        //执行删除
        table.delete(delete);
        table.close();
        System.out.println("删除成功");
    }

    //释放连接
    @After
    public void realse() throws IOException {
        if (conn != null) {
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
