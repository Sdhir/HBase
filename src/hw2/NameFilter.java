
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
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

public class NameFilter {

	public static String Table_Name = "AppData";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		String firstName = "Eric ";
		String year = "2013";
		
		//define the filter
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes("ReviewerInfo"), 
				Bytes.toBytes("reviewerName"),
				CompareOp.EQUAL,
				new SubstringComparator(firstName));
				//new BinaryComparator(Bytes.toBytes(3.0));//overall ratings less than or equal to 3.0		
				
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes("DateTime"), 
				Bytes.toBytes("reviewTime"),
				CompareOp.EQUAL,
				new SubstringComparator(year));
		
		SingleColumnValueFilter filter3 = new SingleColumnValueFilter(
				Bytes.toBytes("Review"), 
				Bytes.toBytes("overall"),
				CompareOp.GREATER_OR_EQUAL,
				new BinaryComparator(Bytes.toBytes(4.0)));
		
		SingleColumnValueFilter filter4 = new SingleColumnValueFilter(
				Bytes.toBytes("Review"), 
				Bytes.toBytes("summary"),
				CompareOp.EQUAL,
				new SubstringComparator("great"));
		
		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL ); //could be FilterList.Operator.MUST_PASS_ALL instead
		filterList.addFilter(filter1);
		filterList.addFilter(filter2);
		filterList.addFilter(filter3);
		filterList.addFilter(filter4);
		
		Scan scan = new Scan();
		scan.setFilter(filterList);
		
		//now we extract the result
		ResultScanner scanner = hTable.getScanner(scan);
		int count = 0;
		
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
			
			String name=new String(result.getValue(
					Bytes.toBytes("ReviewerInfo"),
					Bytes.toBytes("reviewerName")));
			String reviewTime=new String(result.getValue(
					Bytes.toBytes("DateTime"),
					Bytes.toBytes("reviewTime")));
			double overall=Bytes.toDouble(result.getValue(
					Bytes.toBytes("Review"),
					Bytes.toBytes("overall")));
			String summary=new String(result.getValue(
					Bytes.toBytes("Review"),
					Bytes.toBytes("summary")));
			count++;
			System.out.println ("App Reviewer Name: " + name + " Review Time: "+reviewTime+ " Overall rating: "+  overall + " Summary: "+ summary);
		}
		//DecimalFormat df = new DecimalFormat();
		//df.setMaximumFractionDigits(2);
		System.out.println ("Total filtered data: " + count);
		 
		//System.out.println("Average overall rating for App with ASIN: "+ name + " is " + df.format(avg_overall)); 
    }
}

