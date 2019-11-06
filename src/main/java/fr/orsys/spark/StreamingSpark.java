package fr.orsys.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;

public class StreamingSpark {

	public static void main (String[] args) throws AnalysisException, InterruptedException {
		SparkConf conf = new SparkConf().setAppName("Spark Streaming Test").setMaster("local[*]");
		JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(5000));
		
		JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
		        FlumeUtils.createStream(ssc, "localhost", 4444);
		
		JavaDStream<String> javaDStream = flumeStream.map(e -> new String(e.event().getBody().array()));
		
		javaDStream.print();
		
		ssc.start();
		
		ssc.awaitTermination();
		
	}
}
