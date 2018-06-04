package com.mycompany.spark;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests Basic Spark transformation given a data set.
 * 
 * @author dev
 *
 */
public class SparkBasicTest {
	private transient JavaSparkContext sc;
	private SparkBasic sb = null;

	@Before
	public void setUp() {
		sc = new JavaSparkContext("local", "SparkJoinsTest");
		sb = new SparkBasic(sc);
	}

	@After
	public void tearDown() {
		if (sc != null) {
			sc.stop();
		}
	}

	@Test
	public void test() throws IOException {
		Assert.assertNotNull(sb);
		Assert.assertNotNull(sc);
		List<Integer> list = Arrays.asList(new Integer[] { 1, 2, 3, 4, 5 });
		JavaPairRDD<Integer, Integer> result = sb.sumOfSquares(list);
		Assert.assertNotNull(result);
		String filePath = FileUtils.getFile(FileUtils.getUserDirectory(), "results").getAbsolutePath();
		FileUtils.deleteQuietly(FileUtils.getFile(filePath));
		result.saveAsTextFile(filePath);

		assertResults(filePath);
	}

	private void assertResults(String filePath) throws IOException {
		File f = new File(filePath+"/part-00000");
		List<String> lines = FileUtils.readLines(f, "UTF-8");
		Map<Integer, Integer> result = returnResultsMap(lines);
		System.out.println(result);
		Assert.assertEquals(25, result.get(5).intValue());
		Assert.assertEquals(16, result.get(4).intValue());

	}

	private Map<Integer, Integer> returnResultsMap(List<String> lines) {
		return lines.stream()
				.map(p -> p.substring(1, p.length() - 1))
				.map(s -> s.split(","))
				.collect(Collectors.toMap(k -> Integer.getInteger(k[0]), v -> Integer.getInteger(v[1])));
	}

}
