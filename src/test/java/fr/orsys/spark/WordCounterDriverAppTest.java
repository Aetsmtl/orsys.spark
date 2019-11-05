package fr.orsys.spark;

import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.impl.SLF4JLogFactory;
import org.junit.Test;

public class WordCounterDriverAppTest {

	private static Log LOG = new SLF4JLogFactory().getInstance(WordCounterDriverAppTest.class);

	@Test
	public void contextAppTest(){
		
		WordCountDriver.main(null);
		Scanner sc = new Scanner(System.in);

		LOG.info("... \n\nGo to http://lx-8-1:4040 to see the UI :)\nPress any key and then \"Enter\" to quit");
		sc.next();
		sc.close();
	}
	

	@Test
	public void wordCounterDriverTest(){
		WordCountDriver.main(null);
	}
}
