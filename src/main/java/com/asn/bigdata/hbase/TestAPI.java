package com.asn.bigdata.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.math.BigInteger;
import java.util.*;

/**
 * DDL:
 * 1.判断表是否存在
 * 2.创建表
 * 3.创建命名空间
 * 4.删除表
 *
 * DML:
 * 1.插入数据
 * 2.查数据（get）
 * 3.查数据（scan）
 * 4.删除数据
 */
public class TestAPI {
    private static Connection connection = null;
    private static Admin admin = null;

    static {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","flink1,flink2,flink3");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void close() throws IOException {
        if (admin != null){
            admin.close();
        }
        if (connection != null){
            connection.close();
        }
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     * @throws IOException
     */
    public static boolean isTableExist(String tableName) throws IOException {
        //判断表是否存在
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        return exists;
    }

    /**
     * 创建表
     * @param tableName
     * @param columnFamilies
     * @throws IOException
     */
    public static void createTable(String tableName,String... columnFamilies) throws IOException {
        if (columnFamilies.length<=0){
            System.out.println("请设置列族信息，至少有一个列族");
            return;
        }
        if (isTableExist(tableName)){
            System.out.println(tableName+"表已存在！");
            return;
        }
        TableDescriptorBuilder tdesc=TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for(String s: columnFamilies){
            ColumnFamilyDescriptor cfd=ColumnFamilyDescriptorBuilder.of(s);
            tdesc.setColumnFamily(cfd);
        }
        TableDescriptor desc=tdesc.build();
        admin.createTable(desc);
    }

    public static void createTable(String tableName,List<String> columnFamily) throws IOException {
        if (columnFamily.size()==0 || columnFamily == null){
            System.out.println("请设置列族信息，至少有一个列族");
            return;
        }
        if (isTableExist(tableName)){
            System.out.println(tableName+"表已存在！");
            return;
        }
        TableDescriptorBuilder tdesc=TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for(String s: columnFamily){
            ColumnFamilyDescriptor cfd=ColumnFamilyDescriptorBuilder.of(s);
            tdesc.setColumnFamily(cfd);
        }
        TableDescriptor desc=tdesc.build();
        admin.createTable(desc);
    }

    /**
     * 创建命名空间
     * @param namespace
     */
    public static void createNamespace(String namespace) {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (NamespaceExistException e) {
            System.out.println(namespace+"命名空间已存在！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除命名空间
     * @param namespace
     * @param force
     */
    public static void dropNamespace(String namespace, boolean force) {
        try {
            if (force) {
                TableName[] tableNames = admin.listTableNamesByNamespace(namespace);
                for (TableName name : tableNames) {
                    admin.disableTable(name);
                    admin.deleteTable(name);
                }
            }
        } catch (Exception e) {
            // ignore
        }
        try {
            admin.deleteNamespace(namespace);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
    /**
     * 删除表
     * @param tableName
     */
    public static void dropTable(String tableName) throws IOException {
        if (!isTableExist(tableName)){
            System.out.println(tableName+"表不存在");
            return;
        }
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
    }

    /**
     * 插入单条数据
     * @param tableName
     * @param rowkey
     * @param cf
     * @param cn
     * @param value
     * @throws IOException
     */
    public static void putData(String tableName,String rowkey,String cf,String cn,String value) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cn),Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    /**
     * 获取单条数据
     * @param tableName
     * @param rowkey
     */
    public static List<Cell> getDataByKey(String tableName, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = table.get(get);
        for (Cell cell:result.rawCells()){
            System.out.println("cf:"+Bytes.toString(CellUtil.cloneFamily(cell))
            +",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))
            +",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
        }
        List<Cell> listCells = result.listCells();
        table.close();
        return listCells;
    }

    /**
     * scan扫描全表
     * @param tableName
     * @throws IOException
     */
    public static void scanTable(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result:resultScanner){
            for (Cell cell : result.rawCells()) {
                System.out.println("cf:"+Bytes.toString(CellUtil.cloneFamily(cell))
                        +",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                        +",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

    /**
     * 根据rowkey范围扫描过滤
     * @param tableName
     * @param startRow
     * @param stopRow
     */
    public static void scanTable(String tableName,String startRow,String stopRow) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(startRow),Bytes.toBytes(stopRow));
        ResultScanner resultScanner = table.getScanner(scan);

        for (Result result:resultScanner){
            for (Cell cell : result.rawCells()) {
                System.out.println("--------------------------------rowkey:"+Bytes.toString(CellUtil.cloneRow(cell))+",cf:"+Bytes.toString(CellUtil.cloneFamily(cell))
                        +",cn:"+Bytes.toString(CellUtil.cloneQualifier(cell))
                        +",value:"+Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

    /**
     * 根据rowKey过滤数据，rowKey可以使用正则表达式
     * 返回rowKey和Cells的键值对
     * @param tableName
     * @param rowkey
     * @param operator
     * @return
     * @throws IOException
     */
    public static Map<String,List<Cell>> filterByRowKeyRegex(String tableName, String rowkey, CompareOperator operator) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //使用正则
        RowFilter filter = new RowFilter(operator,new RegexStringComparator(rowkey));

        //包含子串匹配(判断一个子串是否存在于值中，并且不区分大小写)
        //RowFilter filter = new RowFilter(operator,new SubstringComparator(rowkey));

        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
        }
        table.close();
        return map;
    }

    /**
     * 根据列族，列名，列值（支持正则）查找数据
     * 返回值：如果查询到值，会返回所有匹配的rowKey下的各列族、列名的所有数据（即使查询的时候这些列族和列名并不匹配）
     * @param tableName
     * @param columnFamily
     * @param columnName
     * @param value
     * @param operator
     * @return
     * @throws IOException
     */
    public static Map<String,List<Cell>> filterByValueRegex(String tableName,String columnFamily,String columnName,
                                                     String value,CompareOperator operator) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();

        //正则匹配
        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
                Bytes.toBytes(columnName),operator,new RegexStringComparator(value));

        //完全匹配
//        SingleColumnValueFilter filter = new SingleColumnValueFilter(Bytes.toBytes(columnFamily),
//                Bytes.toBytes(columnName),operator,Bytes.toBytes(value));

        //SingleColumnValueExcludeFilter排除列值

        //要过滤的列必须存在，如果不存在，那么这些列不存在的数据也会返回。如果不想让这些数据返回,设置setFilterIfMissing为true
        filter.setFilterIfMissing(true);
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
        }
        return map;
    }

    /**
     * 根据列名前缀过滤数据
     * @param tableName
     * @param prefix
     * @return
     * @throws IOException
     */
    public static Map<String,List<Cell>> filterByColumnPrefix(String tableName,String prefix) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));

        //列名前缀匹配
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(prefix));

        //QualifierFilter 用于列名多样性匹配过滤
//        QualifierFilter filter = new QualifierFilter(CompareOperator.EQUAL,new SubstringComparator(prefix));

        //多个列名前缀匹配
//        MultipleColumnPrefixFilter multiFilter = new MultipleColumnPrefixFilter(new byte[][]{});

        Scan scan = new Scan();
        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
        }
        return map;
    }

    /**
     * 根据列名范围以及列名前缀过滤数据
     * @param tableName
     * @param colPrefix
     * @param minCol
     * @param maxCol
     * @return
     * @throws IOException
     */
    public static Map<String,List<Cell>> filterByPrefixAndRange(String tableName,String colPrefix,
                                                         String minCol,String maxCol) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));

        //列名前缀匹配
        ColumnPrefixFilter filter = new ColumnPrefixFilter(Bytes.toBytes(colPrefix));

        //列名范围扫描，上下限范围包括
        ColumnRangeFilter rangeFilter = new ColumnRangeFilter(Bytes.toBytes(minCol),true,
                Bytes.toBytes(maxCol),true);

        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        filterList.addFilter(filter);
        filterList.addFilter(rangeFilter);

        Scan scan = new Scan();
        scan.setFilter(filterList);

        ResultScanner scanner = table.getScanner(scan);
        Map<String,List<Cell>> map = new HashMap<>();
        for(Result result:scanner){
            map.put(Bytes.toString(result.getRow()),result.listCells());
        }
        return map;
    }


    /**
     * 删除数据
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param columnName
     * @throws IOException
     */
    public static void deleteByKeyAndFC(String tableName,String rowkey,String columnFamily,String columnName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        table.delete(delete);
        table.close();
    }

    /**
     * 根据rowKey删除所有行数据
     * @param tableName
     * @param rowkey
     * @throws IOException
     */
    public static void deleteByKey(String tableName,String rowkey) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        table.delete(delete);
        table.close();
    }

    /**
     * 根据rowKey和列族删除所有行数据
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @throws IOException
     */
    public static void deleteByKeyAndFamily(String tableName,String rowkey,String columnFamily) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        delete.addFamily(Bytes.toBytes(columnFamily));
        table.delete(delete);
        table.close();
    }

    /**
     * 根据rowKey、列族删除多个列的数据
     * @param tableName
     * @param rowkey
     * @param columnFamily
     * @param columnNames
     * @throws IOException
     */
    public static void deleteByKeyAndFCList(String tableName,String rowkey, String columnFamily,List<String> columnNames) throws IOException{
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        for(String columnName:columnNames){
            delete.addColumns(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
        }
        table.delete(delete);
        table.close();
    }
    private static byte[][] getSplitKeys() {
        String[] keys = new String[] { "10", "20", "30", "40", "50",
                "60", "70", "80", "90" };
        byte[][] splitKeys = new byte[keys.length][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for (int i = 0; i < keys.length; i++) {
            rows.add(Bytes.toBytes(keys[i]));
        }
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i=0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        return splitKeys;
    }
    /**
     * 创建预分区hbase表
     * @param tableName 表名
     * @param columnFamily 列簇
     * @return
     */
    public static void createTableBySplitKeys(String tableName, List<String> columnFamily,byte[][] splitKeys) throws IOException {
        if (StringUtils.isBlank(tableName) || columnFamily == null
                || columnFamily.size() < 0) {
            System.out.println("===Parameters tableName|columnFamily should not be null,Please check!===");
            return;
        }
        if (isTableExist(tableName)) {
            System.out.println(tableName+"表已存在！");
            return;
        }
        TableDescriptorBuilder tdesc=TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        for(String s: columnFamily){
            ColumnFamilyDescriptor cfd=ColumnFamilyDescriptorBuilder.of(s);
            tdesc.setColumnFamily(cfd);
        }
        TableDescriptor desc=tdesc.build();
        admin.createTable(desc,splitKeys);
        System.out.println("===Create Table " + tableName
                + " Success!columnFamily:" + columnFamily.toString()
                + "===");

    }
    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir","E:\\MySoftware\\hadoop-2.7.5");
        System.out.println("--------------------------------");

        /*putData("table1","0123dd02","info","age","10");
        putData("table1","1021a023","info","age","10");
        putData("table1","2001as12","info","age","10");
        putData("table1","3122pd20","partition1","age","10");
        putData("table1","4025zd00","partition1","age","10");
        putData("table1","5120sd29","partition1","age","10");
        putData("table1","0020fd38","info","age","10");
        putData("table1","16212d10","info","age","10");
        putData("table1","20291d3d","info","age","10");
        putData("table1","3900azsd","partition1","age","10");
        putData("table1","4021adf5","partition1","age","10");
        putData("table1","5403a12e","partition1","age","10");*/
        scanTable("table1","0020","0120");
        close();
    }

}
