package storm.starter.bolt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class SmartPrinterBolt extends PrinterBolt {
	  
	String fileName = "tweets.txt";
	  public SmartPrinterBolt(String filename){
		  fileName = filename;
	  }
	
	  @Override
	  public void execute(Tuple tuple, BasicOutputCollector collector) {
		  File file = new File(fileName);
	      try {
	    	  file.createNewFile();
	  		  FileWriter writer = new FileWriter(file, true); 
	  		  writer.write(tuple.toString() + "\n"); 
		      writer.flush();
		      writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    //System.out.println(tuple);
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer ofd) {
	  }
}
