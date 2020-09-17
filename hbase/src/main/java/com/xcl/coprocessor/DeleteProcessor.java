package com.xcl.coprocessor;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class DeleteProcessor extends BaseRegionObserver{
    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, Durability durability) throws IOException {
        //获取表对象
        HTable table = (HTable) e.getEnvironment().getTable(TableName.valueOf("relationship"));

        //获取被删除的cell
        List<Cell> cells = delete
                .getFamilyCellMap().get(Bytes.toBytes("friends"));
        if(CollectionUtils.isEmpty(cells)){
            table.close();
        }
        //获取第一条数据
        Cell cell = cells.get(0);
        //构建Delete对象
        Delete otherDelete = new Delete(CellUtil.cloneQualifier(cell));
        otherDelete.addColumn(Bytes.toBytes("friends"),CellUtil.cloneRow(cell));

        //执行删除
        table.delete(otherDelete);

        //关闭表对象
        table.close();
    }

}
