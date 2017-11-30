/********************************************************
 * HW#2 | CS 6304
 * 
 * Coded by:
 * Sudhir Sornapudi | Hadi Ataei | Vivekathreya Krishnan
 */

package hw2;

import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
//import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

public class RatingFilteredAsin {

	public static String Table_Name = "AppData";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		String asin = "B004AZH4C8";
		
		//define the filter
		SingleColumnValueFilter filter = new SingleColumnValueFilter(
				Bytes.toBytes("ReviewerInfo"), 
				Bytes.toBytes("asin"),
				CompareOp.EQUAL,
				new SubstringComparator(asin));
				//new BinaryComparator(Bytes.toBytes(3.0));//overall ratings less than or equal to 3.0		
				
		Scan scan = new Scan();
		scan.setFilter(filter);
		
		//now we extract the result
		ResultScanner scanner = hTable.getScanner(scan);
		int count = 0;
		double sum_overall = 0.0;
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
			
			double overall=Bytes.toDouble(result.getValue(
					Bytes.toBytes("Review"),
					Bytes.toBytes("overall")));
			
			sum_overall = sum_overall + overall;
			count++;
		}
		DecimalFormat df = new DecimalFormat();
		df.setMaximumFractionDigits(2);
		double avg_overall = sum_overall/count;
		System.out.println("Total reviews on App with ASIN: "+ asin + " --> " + count); 
		System.out.println("Average overall rating for App with ASIN: "+ asin + " is " + df.format(avg_overall)); 
    }
}

