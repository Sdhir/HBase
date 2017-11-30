/********************************************************
 * HW#2 | CS 6304
 * 
 * Coded by:
 * Sudhir Sornapudi | Hadi Ataei | Vivekathreya Krishnan
 */

package hw2;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;


public class AlterVersion {

public static String Table_Name = "AppData";
	
	@SuppressWarnings("deprecation")
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		
		String row_key = "A1N4O8VOJZTDVBB004A9SDD8";
		//initialize a put with row key as tweet_url
		Put put = new Put(Bytes.toBytes(row_key));

		//insert additional data
		put.add(Bytes.toBytes("Review"), Bytes.toBytes("summary"), Bytes.toBytes("Addicted to this app"));
		hTable.put(put);

		//initialize a get with row key as tweet_url
		Get get = new Get(Bytes.toBytes(row_key));
		get.setMaxVersions(4);

		Result result = hTable.get(get);
		//byte[] b = result.getValue(Bytes.toBytes("Review"), Bytes.toBytes("summary"));
		//System.out.println(new String(b));
		List<KeyValue> allResult = result.getColumn(Bytes.toBytes("Review"), Bytes.toBytes("summary"));
		for(KeyValue kv: allResult) {
			System.out.println(new String(kv.getValue()));
		}
	}
}
