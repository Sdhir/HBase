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

public class LimitRatingsFilter {

	public static String Table_Name = "AppData";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		
		//define the filter
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes("Review"), 
				Bytes.toBytes("overall"),
				CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(Bytes.toBytes(4.0))); //overall ratings greater than or equal to 4.0
		
		Scan scan = new Scan();
		scan.setFilter(filter);
		
		//now we extract the result
		ResultScanner scanner = hTable.getScanner(scan);
		int count = 0, limit =0;
		for(Result result=scanner.next(); result!=null ; result=scanner.next()) {
			if(limit >= 4)
				break;
			double overall=Bytes.toDouble(result.getValue(
					Bytes.toBytes("Review"),
					Bytes.toBytes("overall")));
			
			String asin=new String(result.getValue(
					Bytes.toBytes("ReviewerInfo"),
					Bytes.toBytes("asin")));
			
			String summary=new String(result.getValue(
					Bytes.toBytes("Review"),
					Bytes.toBytes("summary")));
			System.out.println("Overall rating: "+overall + " || ASIN: "+ asin +" || Summary: "+summary);
			count++;
			limit++;
		}
		System.out.println("Total filtered data: "+count); 
    }
}

