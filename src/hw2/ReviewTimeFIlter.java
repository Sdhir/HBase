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
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

public class ReviewTimeFIlter {

	public static String Table_Name = "AppData";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		long unixTime = 1331424000;
		
		//define the filter
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes("Review"), 
				Bytes.toBytes("reviewText"),
				CompareOp.EQUAL,
				new SubstringComparator("fun"));
				//new BinaryComparator(Bytes.toBytes(3.0));//overall ratings less than or equal to 3.0
		
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes("DateTime"), 
				Bytes.toBytes("unixReviewTime"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(unixTime)));//overall ratings less than or equal to 3.0
		
				
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE); //could be FilterList.Operator.MUST_PASS_ALL instead
		filterList.addFilter(filter1);
		filterList.addFilter(filter2);
				
		Scan scan = new Scan();
		scan.setFilter(filterList);
		
		//now we extract the result
		ResultScanner scanner = hTable.getScanner(scan);
		int count = 0;
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
			
			String reviewText=new String(result.getValue(
					Bytes.toBytes("Review"),
					Bytes.toBytes("reviewText")));
			
			String asin=new String(result.getValue(
					Bytes.toBytes("ReviewerInfo"),
					Bytes.toBytes("asin")));
			
			long unixReviewTime=Bytes.toLong(result.getValue(
					Bytes.toBytes("DateTime"),
					Bytes.toBytes("unixReviewTime")));
			
			System.out.println("ASIN: "+ asin + " || UnixReviewTime: "+ unixReviewTime+" || Review Text: "+ reviewText );
			count++;
		}
		System.out.println("Total filtered data: "+count); 
    }
}

