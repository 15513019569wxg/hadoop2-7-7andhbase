package hbaseclient;/*
    @author wxg
    @date 2021/7/15-11:27
    */

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

public class coprocessor extends BaseRegionObserver {
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //  1、获取connection对象
        Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
        //  2、获取index对象
        Table index = connection.getTable(TableName.valueOf("index"));
        //  3、执行数据插入
        index.put(put);
        //  4、关闭资源
        index.close();
        connection.close();
    }
}
