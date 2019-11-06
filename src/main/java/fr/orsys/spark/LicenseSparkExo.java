package fr.orsys.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.AnalysisException;

public class LicenseSparkExo {

	public static void main (String[] args) throws AnalysisException {
		run();
	}
	
	public static void run () throws AnalysisException {
		
		SparkSession session = getSession();
				
		Dataset<Row> csvLicenses = session.read()
										  .option("header", true)
										  .csv("Licences_2015.csv");
		
		// csvLicenses.show();
		
		Dataset<Row> fede = session.read()
				  .option("header", true)
				  .option("inferSchema", true) // test without
				  .csv("Licences_2015.csv")
				  .select(col("fed_2014"), col("l_2015"), col("LIBELLE"))
				  .where(col("LIBELLE").like("Paris"))
				  .orderBy(col("l_2015").desc());
				  
				  ;
		
				  Dataset<Row> select = session.read()
				  .option("header", true)
				  .option("inferSchema", true) // test without
				  .csv("Licences_2015.csv")
				  .select(col("fed_2014"), col("l_2015"));
				  
				  select.createTempView("licences");
				  
				  select.printSchema();
				  
//				  Dataset<Row> sql = session.sql("select * from licences");
				  
				  Dataset<Row> select2 = session.read()
				  		 .option("header", true)
				  		 .csv("Code_federation_-_fichiers_licences.csv")
				  		 .select(col("code fede").as("code"), col("Fédérations françaises agréées").as("libelle"));
				
				  select2.createOrReplaceTempView("codes");
				  
				  select2.printSchema();
//				  session.sql("select * from codes")
//				  		 .show(false);
				  
				  Dataset<Row> sql2 = session.sql("select code, libelle, sum(l_2015) as nb from licences "
				  		+ "join codes on licences.fed_2014 = codes.code "
				  		+ "group by code, libelle order by nb desc");
				  
				  sql2.show(false);
				  
				  sql2.write()
				  	  .format("jdbc")
				  	  .option("user", "root")
				  	  .option("password", "root")
				  	  .option("driver", "com.mysql.jdbc.Driver")
				  	  .option("url", "jdbc:mysql://localhost:3306/spk")
				  	  .option("dbTable", "stat_licences")
				  	  .mode(SaveMode.Append)
				  	  .save();
				  	  ;
				 
		//fede.show();
				  
	}

	private static SparkSession getSession() {
		SparkSession session = 
				SparkSession.builder()
							.master("local[*]")
							.appName("LicenseSQL")
							.getOrCreate();
		return session;
	}
}
