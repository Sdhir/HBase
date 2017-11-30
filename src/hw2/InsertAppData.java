/********************************************************
 * HW#2 | CS 6304
 * 
 * Coded by:
 * Sudhir Sornapudi | Hadi Ataei | Vivekathreya Krishnan
 */

package hw2;

import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class InsertAppData extends Configured implements Tool{

	public String Table_Name = "AppData";
    @SuppressWarnings({ "deprecation", "unchecked" })
	@Override
    public int run(String[] argv) throws IOException {
        Configuration conf = HBaseConfiguration.create();        
        @SuppressWarnings("resource")
		HBaseAdmin admin=new HBaseAdmin(conf);    
        JSONParser parser = new JSONParser();
        
        boolean isExists = admin.tableExists(Table_Name);
        
        if(isExists == false) {
	        //create table with column family
	        HTableDescriptor htb=new HTableDescriptor(Table_Name);
	        HColumnDescriptor ReviewerInfoFamily = new HColumnDescriptor("ReviewerInfo");
	        HColumnDescriptor HelpfulFamily = new HColumnDescriptor("Helpful");
	        HColumnDescriptor ReviewFamily = new HColumnDescriptor("Review");
	        HColumnDescriptor DateTimeFamily = new HColumnDescriptor("DateTime");
	        
	        htb.addFamily(ReviewerInfoFamily);
	        htb.addFamily(HelpfulFamily);
	        htb.addFamily(ReviewFamily);
	        htb.addFamily(DateTimeFamily);
	        admin.createTable(htb);
        }
        
        try {
    		@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new FileReader("Apps_for_Android_5.json"));
    	    String line;
    	    
    	    int row_count=0;
    	    
    	    //iterate over every line of the input file
    	    while((line = br.readLine()) != null) {
    	    	
    	    	if(line.isEmpty())continue;
    	    	
    	    	row_count++;

    	    	Object obj = parser.parse(line);
                JSONObject jsonObject = (JSONObject) obj;

                String reviewerID = (String) jsonObject.get("reviewerID");
                //System.out.println(reviewerID);

                String asin = (String) jsonObject.get("asin");
                //System.out.println(asin);

                String reviewerName = (String) jsonObject.get("reviewerName");
                if(reviewerName == null){
                	reviewerName = "NA";
                }
                //System.out.println(reviewerName);
                
                JSONArray helpful = (JSONArray) jsonObject.get("helpful");
                Iterator<Long> iterator = helpful.iterator();
                long foundHelp =  iterator.next();
                long total =  iterator.next();
                //System.out.println(foundHelp);
                //System.out.println(Total);
                /*
                while (iterator.hasNext()) {
                	System.out.println(iterator.next());
                    }
                */
                
                String reviewText = (String) jsonObject.get("reviewText");
                
                double overall = (double)jsonObject.get("overall");
                //System.out.println(overall);
                
                String summary = (String) jsonObject.get("summary");
                
                long unixReviewTime = (long) jsonObject.get("unixReviewTime");
                
                String reviewTime = (String) jsonObject.get("reviewTime");
                
    	    	//initialize a put with row key as reviewerID and asin
	            Put put = new Put(Bytes.toBytes(reviewerID + asin));
	            
	            //add column data one after one
	            put.add(Bytes.toBytes("ReviewerInfo"), Bytes.toBytes("reviewerID"), Bytes.toBytes(reviewerID));
	            put.add(Bytes.toBytes("ReviewerInfo"), Bytes.toBytes("asin"), Bytes.toBytes(asin));
	            put.add(Bytes.toBytes("ReviewerInfo"), Bytes.toBytes("reviewerName"), Bytes.toBytes(reviewerName));
	            
	            put.add(Bytes.toBytes("Helpful"), Bytes.toBytes("foundHelp"), Bytes.toBytes(foundHelp));
	            put.add(Bytes.toBytes("Helpful"), Bytes.toBytes("total"), Bytes.toBytes(total));
	            
	            
	            put.add(Bytes.toBytes("Review"), Bytes.toBytes("reviewText"), Bytes.toBytes(reviewText));
	            put.add(Bytes.toBytes("Review"), Bytes.toBytes("overall"), Bytes.toBytes(overall));
	            put.add(Bytes.toBytes("Review"), Bytes.toBytes("summary"), Bytes.toBytes(summary));
	            
	            put.add(Bytes.toBytes("DateTime"), Bytes.toBytes("unixReviewTime"), Bytes.toBytes(unixReviewTime));
	            put.add(Bytes.toBytes("DateTime"), Bytes.toBytes("reviewTime"), Bytes.toBytes(reviewTime));
	            
	            //add the put in the table
    	    	HTable hTable = new HTable(conf, Table_Name);
    	    	hTable.put(put);
    	    	hTable.close();    
	    	}
    	    System.out.println("Inserted " + row_count + " rows");
    	    
	    } catch (FileNotFoundException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } catch (IOException e) {
	    	// TODO Auto-generated catch block
	    	e.printStackTrace();
	    } catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		}

      return 0;
   }
    
    public static void main(String[] argv) throws Exception {
        int ret = ToolRunner.run(new InsertAppData(), argv);
        System.exit(ret);
    }
}
