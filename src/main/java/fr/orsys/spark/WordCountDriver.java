package fr.orsys.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SLF4JLog;
import org.apache.commons.logging.impl.SLF4JLogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCountDriver {

	private static Log LOG = new SLF4JLogFactory().getInstance(WordCountDriver.class);
	
	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount");
		JavaSparkContext context = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputTextFile = context.textFile("sherlock.txt");
		long count = inputTextFile.count();
	}
}
