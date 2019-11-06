package fr.orsys.spark;

import org.apache.spark.sql.AnalysisException;
import org.junit.Test;

public class LicenseSparkExoTest { 

	private LicenseSparkExo licenseSpark = new LicenseSparkExo();
	
	@Test
	public void licenseSparkExoTest () throws AnalysisException {
		licenseSpark.run();
	}
}
