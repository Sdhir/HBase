/********************************************************
 * HW#2 | CS 6304
 * 
 * Coded by:
 * Sudhir Sornapudi | Hadi Ataei | Vivekathreya Krishnan
 */

package hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HelpfulReview {

	public static String Table_Name = "AppData";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		long t = 5;
		
		//define the filter
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes("Helpful"), 
				Bytes.toBytes("total"),
				CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(Bytes.toBytes(t))); //reviews provided by #people greater than or equal to 5
		
		Scan scan = new Scan();
		scan.setFilter(filter);
		
		//now we extract the result
		ResultScanner scanner = hTable.getScanner(scan);
		int count = 0;
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
			long foundHelp = Bytes.toLong(result.getValue(
					Bytes.toBytes("Helpful"),
					Bytes.toBytes("foundHelp")));
			
			long total = Bytes.toLong(result.getValue(
					Bytes.toBytes("Helpful"),
					Bytes.toBytes("total")));
			
			String asin=new String(result.getValue(
					Bytes.toBytes("ReviewerInfo"),
					Bytes.toBytes("asin")));
			
			System.out.println("The review rating for the product ASIN: "+ asin + "is "+ ((float)foundHelp/total)*100 + "% helpful");
			count++;
		}
		System.out.println("Number of helpful reviews: "+count); 
    }
}

