package edu.easternct.bigdata;


import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import com.holdenkarau.spark.testing.JavaRDDComparisons;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import edu.easternct.bigdata.*;
import scala.Option;
import scala.Serializable;
import scala.Tuple2;

import java.math.BigDecimal;
import java.math.MathContext;

import scala.reflect.ClassTag;

import org.junit.Test;


public class TychoTest extends SharedJavaSparkContext {
	
	/**
	 * 
	 */
	

	@Test
	public void verifyValidStars() {
	    // Create and run the test
		System.out.println("Verify Valid Stars");
		
	    JavaRDD<String> inputRDD = jsc().textFile("/home/training/training_materials/Spark/data/tyc2.test.dat/tyc2.dat.00");
	    JavaRDD<Star> result = TychoAnalysis4.findValidStars(inputRDD);
	    System.out.println("Input Count: " +result.count());
	                                          

	    // Create the expected output
	    
	    Star temp1 = new Star("0001 00008 1", " ",2.31750494,2.23184345,new BigDecimal("12.146",new MathContext(5)),new BigDecimal("12.146",new MathContext(5)), " ");
	    Star temp2 = new Star("0001 00013 1", " ",1.12558209,2.267394,new BigDecimal(10.488,new MathContext(5)),new BigDecimal(8.67,new MathContext(5))," ");
	    Star temp3 = new Star("0001 00016 1", " ",1.0568649,1.8978287,new BigDecimal(12.921,new MathContext(5)),new BigDecimal(12.10,new MathContext(5))," ");
	    Star temp6 = new Star("0001 00020 1", " ",0.37220148,2.48018803,new BigDecimal(12.193,new MathContext(5)),new BigDecimal(11.398,new MathContext(5))," ");
	    Star temp7 = new Star("0001 00022 1", " ",1.02766471,1.45849152,new BigDecimal(12.682,new MathContext(5)),new BigDecimal(12.289,new MathContext(5))," ");
	    Star temp8 = new Star("0001 00024 1", " ",0.77431739,1.71059531,new BigDecimal(12.537,new MathContext(5)),new BigDecimal(11.697,new MathContext(5))," ");
	    Star temp9 = new Star("0001 00036 1", " ",0.29838646,1.92361761,new BigDecimal(11.672,new MathContext(5)),new BigDecimal(11.648,new MathContext(5))," ");
	    Star temp10 = new Star("0001 00039 1", " ",2.37141849,2.30400355,new BigDecimal(9.51,new MathContext(5)),new BigDecimal(9.387,new MathContext(5))," ");
	    
	    List<Star> expectedInput = Arrays.asList(temp1, temp2, temp3, temp6, temp7, temp8, temp9, temp10);

	    expectedInput.forEach(s -> {s.setRaBin(); s.setBMinV();}); 
	    
	     JavaRDD<Star> expectedRDD = jsc().parallelize(expectedInput);
	     
	     System.out.println("Spark Generated data: " +result.count());
	     result.foreach(System.out::println);
	     System.out.println("Expected Generated data: " +expectedRDD.count());
	     expectedRDD.foreach(System.out::println);
	     

	    ClassTag<Star> tag =
	        scala.reflect.ClassTag$.MODULE$
	        .apply(Star.class);

	    // Run the assertions on the result and expected
	    JavaRDDComparisons.assertRDDEquals(
	        JavaRDD.fromRDD(JavaRDD.toRDD(result), tag),
	        JavaRDD.fromRDD(JavaRDD.toRDD(expectedRDD), tag));
	}

	@Test
	public void verifyBinStarCount() {
		
	    System.out.println("Verify Bin Star Count");
	    
	    // Create and run the test
	    
	    JavaRDD<String> inputRDD = jsc().textFile("/home/training/training_materials/Spark/data/tyc2.test.dat/tyc2.dat.00");
	    JavaRDD<Star> result = TychoAnalysis4.findValidStars(inputRDD);
	    JavaPairRDD<Integer, Integer> binStarCounts = TychoAnalysis4.computeBinStarCounts(result);
	    
	    // Create the expected output
	    
	    Tuple2<Integer, Integer> temp1 = new Tuple2<Integer, Integer>(1, 3);
	    Tuple2<Integer, Integer> temp2 = new Tuple2<Integer, Integer>(2, 3);
	    Tuple2<Integer, Integer> temp3 = new Tuple2<Integer, Integer>(3, 2);
	    
//	    RaBin temp2 = new RaBin(2, 3, 2.57720000);
//	    RaBin temp3 = new RaBin(3, 3, 0.10455000);
	    
	    List<Tuple2<Integer, Integer>> expectedInput = Arrays.asList(temp1, temp2, temp3);

 
	    
	     JavaPairRDD<Integer, Integer> expectedRDD = jsc().parallelizePairs(expectedInput);
	     
	     System.out.println("Spark Generated data: " +binStarCounts.count());
	     binStarCounts.foreach(System.out::println);
	     System.out.println("Expected Generated data: " +expectedRDD.count());
	     expectedRDD.foreach(System.out::println);
	     

	    ClassTag<Tuple2<Integer, Integer>> tag =
	        scala.reflect.ClassTag$.MODULE$
	        .apply(Tuple2.class);

	    // Run the assertions on the result and expected
	    JavaRDDComparisons.assertRDDEquals(
	        JavaRDD.fromRDD(JavaPairRDD.toRDD(binStarCounts), tag),
	        JavaRDD.fromRDD(JavaPairRDD.toRDD(expectedRDD), tag));
	}
	@Test
	public void verifyBinColorIndexes() {
		
	    System.out.println("Verify Bin Color Indexes");
	    
	    // Create and run the test
	    
	    JavaRDD<String> inputRDD = jsc().textFile("/home/training/training_materials/Spark/data/tyc2.test.dat/tyc2.dat.00");
	    JavaRDD<Star> result = TychoAnalysis4.findValidStars(inputRDD);
	    JavaPairRDD<Integer, BigDecimal> starColorIndexes = TychoAnalysis4.getColorIndexDetails(result);
	    JavaPairRDD<Integer, BigDecimal> binColorIndexes = TychoAnalysis4.computeTotalColorIndex(starColorIndexes);
	    
	    // Create the expected output
	    
	    Tuple2<Integer, BigDecimal> temp1 = new Tuple2<>(1, new BigDecimal("1.410150",new MathContext(6)));
	    Tuple2<Integer, BigDecimal> temp2 = new Tuple2<>(2, new BigDecimal("2.577200",new MathContext(6)));
	    Tuple2<Integer, BigDecimal> temp3 = new Tuple2<>(3, new BigDecimal("0.104550",new MathContext(6)));
	    
	    List<Tuple2<Integer, BigDecimal>> expectedInput = Arrays.asList(temp1, temp2, temp3);

 
	    
	     JavaPairRDD<Integer, BigDecimal> expectedRDD = jsc().parallelizePairs(expectedInput);
	     
	     System.out.println("Spark Generated data: " +binColorIndexes.count());
	     binColorIndexes.foreach(System.out::println);
	     System.out.println("Expected Generated data: " +expectedRDD.count());
	     expectedRDD.foreach(System.out::println);
	     

	    ClassTag<Tuple2<Integer, BigDecimal>> tag =
	        scala.reflect.ClassTag$.MODULE$
	        .apply(Tuple2.class);

	    // Run the assertions on the result and expected
	    JavaRDDComparisons.assertRDDEquals(
	        JavaRDD.fromRDD(JavaPairRDD.toRDD(binColorIndexes), tag),
	        JavaRDD.fromRDD(JavaPairRDD.toRDD(expectedRDD), tag));
	}

	@Test
	public void verifyRaBinCreation() {
		
	    System.out.println("Verify RA Bin Creation");
	    
	    // Create and run the test
	    
	    JavaRDD<String> inputRDD = jsc().textFile("/home/training/training_materials/Spark/data/tyc2.test.dat/tyc2.dat.00");
	    JavaRDD<Star> result = TychoAnalysis4.findValidStars(inputRDD);
	    JavaPairRDD<Integer, Integer> binStarCounts = TychoAnalysis4.computeBinStarCounts(result);
	    JavaPairRDD<Integer, BigDecimal> starColorIndexes = TychoAnalysis4.getColorIndexDetails(result);
	    JavaPairRDD<Integer, BigDecimal> binColorIndexes = TychoAnalysis4.computeTotalColorIndex(starColorIndexes);
	    JavaPairRDD<Integer, RaBin> binStats = TychoAnalysis4.groupAndCollect(starColorIndexes);
	    JavaRDD<RaBin> outputRDD = TychoAnalysis4.createBins(binStarCounts, binColorIndexes,binStats);
	    
	    System.out.println("Got this far #1: " +outputRDD.count());
	    // Create the expected output
	    
	    RaBin temp1 = new RaBin(1, 3, new BigDecimal("1.410150"));
	    RaBin temp2 = new RaBin(2, 3, new BigDecimal("2.5772"));
	    RaBin temp3 = new RaBin(3, 2, new BigDecimal("0.104550"));

	    
	    List<RaBin> expectedInput = Arrays.asList(temp1, temp2, temp3);

 
	    System.out.println("Got this far #2: " +expectedInput.size());
	    JavaRDD<RaBin> expectedRDD = jsc().parallelize(expectedInput);
	    System.out.println("Got this far #3: " +expectedRDD.count()); 
	    
	    System.out.println("Spark Generated data: " +outputRDD.count());
	    outputRDD.foreach((RaBin r) -> System.out.println(r.toFormattedString()));
	    System.out.println("Expected Generated data: " +expectedRDD.count());
	    expectedRDD.foreach((RaBin r) -> System.out.println(r.toFormattedString()));
	     

	    ClassTag<RaBin> tag =
	        scala.reflect.ClassTag$.MODULE$
	        .apply(RaBin.class);

	    // Run the assertions on the result and expected
	    JavaRDDComparisons.assertRDDEquals(
	        JavaRDD.fromRDD(JavaRDD.toRDD(outputRDD), tag),
	        JavaRDD.fromRDD(JavaRDD.toRDD(expectedRDD), tag));
	}

	@Test
	public void verifyCollectStats() {
		
	    System.out.println("Verify Collect Stats");
	    
	    // Create and run the test
	    
	    BigDecimal data1 = new BigDecimal("100.50");
	    BigDecimal data2 = new BigDecimal("50.50");
	    BigDecimal data3 = new BigDecimal("1");
	    BigDecimal data4 = new BigDecimal("175.00");
	    BigDecimal data5 = new BigDecimal("40.00");
	    
	    List<BigDecimal> input = new ArrayList<>();
	    input.add(data1);
	    input.add(data2);
	    input.add(data3);
	    input.add(data4);
	    input.add(data5);
	    
	    List<Double> results = TychoAnalysis4.collectStats(1, input);
	    
	    // Create the expected output
	    
	    StringBuffer expected = new StringBuffer();
	    expected.append("limited Entires length: 3/n");
	    expected.append("RaBin : 1 Mean : 180.517 Minimum : 180.517 Maximum : 180.517 Median : 180.517 Standard Deviation : 0.0/n");
	    assert(results.get(0).equals(new Double(91.75)));     //mean
	    assert(results.get(1).equals(new Double(50.50)));     //median
	    assert(results.get(2).equals(new Double(25.00)));     //sd
	    assert(results.get(3).equals(new Double(1.00)));      //min
	    assert(results.get(4).equals(new Double(175.00)));    //max
	    assert(results.get(5).equals(new Double(1.00)));      // ??

	    
	  }

	
	
}
