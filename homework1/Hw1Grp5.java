import java.util.*;
import java.lang.Comparable;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.log4j.*;

/**
 * zhangqiuping 201618013229025
 *
 * Hw1Grp5, sort based distinct
 *
 * @author zhangqiuping
 *
 * @version 1.0.0
 */

public class Hw1Grp5 {

    public static String tableName= "Result";  //table name
    public static String ColumnFamily= "res";  //ColumnFamily name
    public static String []column;  //column name
    public static int[] columnNo;  //column number
    public static ArrayList<String> reslist;  //result data
    public static String ComSymbol;  //compare symbols, inculding gt(>),ge(>=),eq(==),ne(!=),le(<=),lt(<)
    public static double ComNo;  //compare number

    /**
     * this method is the main entrance
     * @param args command line string
     * @return void
     * @exception IOExpection throw when something is wrong with I/O
     * @exception URISyntaxException throw when something is wrong with URI
     */

    public static void main(String[] args) throws IOException, URISyntaxException{

        //java Hw1Grp5 R=<file> select:R1,gt,5.1 distinct:R2,R3,R5

        String file= "hdfs://localhost:9000" + args[0].substring(2);  //the file path
        ComSymbol = args[1].split(":")[1].split(",")[1]; //compare symbols, inculding gt(>),ge(>=),eq(==),ne(!=),le(<=),lt(<)
        column = args[2].split(":")[1].split(",");  //column name (R2,R3,R5)
        int ComColumnNo = Integer.parseInt(args[1].substring(args[1].indexOf("R")+1,args[1].indexOf(",")));//compare column number R1(1)
        ComNo = Double.valueOf(args[1].split(":")[1].split(",")[2]); //compare number (5.1)
        int columnlen = column.length;  //the length of column


        columnNo = new int[columnlen];  //column number with column length R2(2),R3(3),R5(5)
        for(int i=0; i<columnlen; i++){
            //column number
            columnNo[i] = Integer.parseInt(column[i].substring(column[i].indexOf("R")+1));
        }

        /*System.out.println("compare column number: " + ComColumnNo);
        System.out.println("compare symbols: " + ComSymbol);
        System.out.println("compare number: " + ComNo);
        System.out.println("column name: ");
        for (int p=0;p<columnlen;p++){
            System.out.println(column[p]);
        }

        System.out.println("columnlen: " + columnlen);
        System.out.println("column number: ");
        for (int q=0;q<columnlen;q++){
            System.out.println(columnNo[q]);
        }*/

        // hdfs
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);

        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;
        reslist=new ArrayList<String>();

        while ((s=in.readLine())!=null) {
            String[] str=s.split("\\|");  //character segmentation with "|"

            //double disNo=Double.valueOf(str[ComColumnNo]);

            //judge ComSymbol gt(>),ge(>=),eq(==),ne(!=),le(<=),lt(<)
            if(judge(Double.valueOf(str[ComColumnNo]))){
                String restmp="";  //save the data of oneline

                for(int j=0; j<columnlen; j++){
                    if(restmp.equals("")){
                        restmp=str[columnNo[j]];
                    }
                    else
                        restmp=restmp + "|" +str[columnNo[j]];

                }
                //System.out.println(restmp);
                reslist.add(restmp);
            }
        }

        in.close();
        fs.close();

        //sort
        Object[] sortRes=reslist.toArray();
        Arrays.sort(sortRes);
        reslist.clear();
        reslist.add(sortRes[0].toString());

        //distinct
        //int discount=0;  //the number of distinct row
        for(int n=1; n<sortRes.length; n++){
            if(!sortRes[n].toString().equals(sortRes[n-1].toString()))
                reslist.add(sortRes[n].toString());
                //discount++;
        }

        for(String list:reslist){
            System.out.print(list+"\n");
        }
        //System.out.println("discount=:" + discount);

        // create hbase table
        Logger.getRootLogger().setLevel(Level.WARN);

        // create table descriptor
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));

        // create column descriptor
        HColumnDescriptor cf = new HColumnDescriptor(ColumnFamily);
        htd.addFamily(cf);

        // configure HBase
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin hAdmin = new HBaseAdmin(configuration);

        if (hAdmin.tableExists(tableName)) {
            System.out.println("Table already exists");
            hAdmin.disableTable(tableName);
            hAdmin.deleteTable(tableName);
            System.out.println("delete table ok");
        }

        hAdmin.createTable(htd);
        System.out.println("table "+tableName+ " created successfully");

        hAdmin.close();

        //put "Result", row key:1,2,3... , res:R2, R3, R5..., value

        HTable table = new HTable(configuration,tableName);

        int rowKey=0;
        for(String ans:reslist){
	    Put put=new Put((rowKey+"").getBytes());
	    String []result=ans.split("\\|");
	    for(int i=0;i<result.length;++i){
	        put.add(ColumnFamily.getBytes(),(column[i]).getBytes(),result[i].getBytes());
	    }
	    table.put(put);
	    ++rowKey;
	}

        /*for(int m=0; m<discount; m++){
            String[] s3 = String.valueOf(reslist.get(m)).split("\\|");

            Put put = new Put(Bytes.toBytes(String.valueOf(m)));

            for(int k=0; k<columnlen; k++){
                put.add(Bytes.toBytes(ColumnFamily), Bytes.toBytes(column[k]), Bytes.toBytes(s3[k]));
            }

            /*put.add(Bytes.toBytes(ColumnFamily), Bytes.toBytes("R13"), Bytes.toBytes(s3[0]));
            put.add(Bytes.toBytes(ColumnFamily), Bytes.toBytes("R14"), Bytes.toBytes(s3[1]));
            put.add(Bytes.toBytes(ColumnFamily), Bytes.toBytes("R8"), Bytes.toBytes(s3[2]));
            put.add(Bytes.toBytes(ColumnFamily), Bytes.toBytes("R9"), Bytes.toBytes(s3[3]));

            table.put(put);
        }*/

        table.close();
        System.out.println("put successfully");

    }


    //judge ComSymbol gt(>),ge(>=),eq(==),ne(!=),le(<=),lt(<)
    public static boolean judge(double distinctNo){
        if(ComSymbol.equals("gt"))
            return distinctNo>ComNo;

        if(ComSymbol.equals("ge"))
            return distinctNo>=ComNo;

        if(ComSymbol.equals("eq"))
            return distinctNo==ComNo;

        if(ComSymbol.equals("ne"))
            return distinctNo!=ComNo;

        if(ComSymbol.equals("le"))
            return distinctNo<=ComNo;

        if(ComSymbol.equals("lt"))
            return distinctNo<ComNo;

        //defaule
        return false;

    }

}
