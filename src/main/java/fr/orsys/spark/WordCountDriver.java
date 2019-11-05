package fr.orsys.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SLF4JLogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.json.JSONArray;

import scala.Tuple2;

public class WordCountDriver {

	private static Log LOG = new SLF4JLogFactory().getInstance(WordCountDriver.class);
	
	static SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount");
	static JavaSparkContext context = new JavaSparkContext(sparkConf);
	
	public static void main(String[] args) {
		
		JavaRDD<String> inputTextFile = context.textFile("sherlock.txt");
		
		JavaRDD<String> flatMapLine = inputTextFile.flatMap(new SplitWordFunction());
		
		JavaRDD<String> inputStopWords = context.textFile("stop-words-en.json");
		
		JavaRDD<String> stopWords = inputStopWords.flatMap(array -> {
			JSONArray a = new JSONArray(array);
			List<String> list = new ArrayList<String>();
			a.forEach(o -> list.add(((String)o)));
			return list.iterator();
		});
		
		JavaPairRDD<String, Integer> wordsPair = 
				flatMapLine.filter(w -> !w.isEmpty())
						   .map(w ->  w.toLowerCase())
						   .map(w -> w.replaceAll("\\W", ""))
						   .subtract(stopWords)
						   .mapToPair(new WordToPairFunction());
				;
		
		//JavaPairRDD<String, Integer> mapToPair = stopWords.mapToPair(new WordToPairFunction());
		
		// mapToPair.reduceByKey((x, y) -> x + y);
		JavaPairRDD<String, Integer> reduceByKey = wordsPair.reduceByKey(Integer::sum);		
		
		// -3
		reduceByKey.mapToPair(t -> t.swap())
				   .sortByKey(false)
				   .take(10)
				   .forEach(LOG::info);
		
//		// 1
//		reduceByKey.mapToPair(t -> t.swap())
//				   .sortByKey()
//				   .collect() // bring all elements to master (driver) before sort it
//				   .forEach(LOG::info);
//		// 2
//		reduceByKey.mapToPair(t -> t.swap())
//				   .sortByKey()
//				   .foreach(LOG::info); // sort elements directly on slave then print it
		
//		// 3
//		reduceByKey.mapToPair(t -> t.swap())
//				   .sortByKey()
//				   .saveAsTextFile("mapToPairResult.txt");
		
	
//		long countInputTextFile = inputTextFile.count();
//		LOG.info("0 - countInputTextFile count from JavaRDD : " + countInputTextFile);
//		
//		long countFlatMapLine = flatMapLine.count();
//		LOG.info("1 - countFlatMapLine count from JavaPairRDD : " + countFlatMapLine);
//		
//		long countMapToPair = mapToPair.count();
//		LOG.info("2 - countMapToPair count from JavaPairRDD : " + countMapToPair);
		
//		long countReduceByKey = reduceByKey.count();
//		LOG.info("3 - countReduceByKey count from JavaPairRDD : " + countReduceByKey);
		
	}
	
	protected static class SplitWordFunction implements FlatMapFunction<String, String> {
		
		// Création d'un tabeau de mots
		
		@Override
		public Iterator<String> call(String line) throws Exception {
			String[] words = line.split(" ");
			return Arrays.asList(words).iterator();
		}
	}

	protected static class WordToPairFunction implements PairFunction<String, String, Integer> {
		
		// Application des poids (1) à chaque mot.
		
		@Override
		public Tuple2<String, Integer> call(String word) throws Exception {
			return new Tuple2<String, Integer>(word, 1);
		}
	}
}
