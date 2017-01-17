package edu.easternct.bigdata;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.math3.stat.*;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import scala.Tuple2;

public class TychoAnalysis4 {

	public static void main(String[] args) throws Exception {

   	     SparkConf conf = new SparkConf();
	     conf.set("spark.app.name", "TychoAnalysis");
	     conf.set("spark.master", args[0]);
	     JavaSparkContext sc = new JavaSparkContext(conf);
   
   
         JavaRDD<String> rdd1 = sc.textFile(args[1]);
         JavaRDD<Star> declRDD = findValidStars(rdd1);
         JavaPairRDD<Integer, Integer> binStarCounts =  computeBinStarCounts(declRDD);
         JavaPairRDD<Integer, BigDecimal> starColorIndexes =  getColorIndexDetails(declRDD);
         JavaPairRDD<Integer, BigDecimal> binColorIndexes =  computeTotalColorIndex(starColorIndexes);

         JavaPairRDD<Integer, RaBin> binStats = groupAndCollect(starColorIndexes);
         
         JavaRDD<RaBin> outputRDD =  createBins(binStarCounts, binColorIndexes, binStats);
         
         // Send Data to output
         outputRDD.coalesce(1).saveAsTextFile(args[2]);
         
         
         binStats.foreach(System.out::println);
         
         sc.close();
  }

	public static JavaRDD<Star> findValidStars(JavaRDD<String> input) {

        //  Helper Functions
        //  Function to Map Data to Star Objects	     
        Function<String, Star> mapStars = (String x) -> {String[] data = x.split("\\|");
                                                   Star temp = new Star();
                                                   temp.setTychoId(data[0]);
                                                   temp.setMeanFlag(data[1]);
                                                   temp.setRaMdeg(Double.parseDouble(data[2].trim()));
                                                   temp.setDeMdeg(Double.parseDouble(data[3].trim()));
                                                   temp.setBtMag(new BigDecimal(data[17].trim()));
                                                   temp.setVtMag(new BigDecimal(data[19].trim()));
                                                   temp.setHipNum(data[23]);
                                                   temp.setRaBin();
                                                   temp.setBMinV();
                                                   return temp;
   												};

   		//  Function to Validate Star Data on Input    
         Function<String, Boolean> validate = (String x) -> {StarValidator edit = new StarValidator(x);
                                                   edit.parser();
                                                   return edit.isClean();		                                                   
   												}; 												

		JavaRDD<String> validStrings = input.filter(validate);
        JavaRDD<Star>   validStars = validStrings.map(mapStars)
		                                           .filter(p -> p.getMeanFlag().equals(" "))
		                                           .distinct();

        // Filter out stars with a Declination less than zero or greater than 10
        JavaRDD<Star> declRDD = validStars.filter(q -> q.getDeMdeg() > 0 && q.getDeMdeg() < 10);

        return declRDD; 	    	    

	  }

	static JavaPairRDD<Integer, Integer> computeBinStarCounts(JavaRDD<Star> stars) {
		
		// Build count Tuple - each row representing a star in a bin
        JavaPairRDD<Integer,Integer> bin3RDD = stars.mapToPair(s -> new Tuple2<Integer, Integer>(s.getRaBin(), 1));

        // Reduce all rows to bin counts
        JavaPairRDD<Integer, Integer> binCountRDD = bin3RDD.reduceByKey((x,y) -> x + y);
        
        return binCountRDD;    	
        	    

	  }
	
	  static JavaPairRDD<Integer, BigDecimal> getColorIndexDetails(JavaRDD<Star> stars) {
		
		// Build B-V tuple by bin key for each star
	    JavaPairRDD<Integer,BigDecimal> bin2RDD = stars.mapToPair(s -> new Tuple2<Integer, BigDecimal>(s.getRaBin(), s.getBMinV()));

	    return bin2RDD;
	    
	  }
	  
	  static JavaPairRDD<Integer, BigDecimal> computeTotalColorIndex(JavaPairRDD<Integer, BigDecimal> starData) { 
        
		// Reduce all rows to bin counts
	    JavaPairRDD<Integer, BigDecimal> bMinVSumRDD = starData.reduceByKey((x,y) -> x.add(y,new MathContext(6)));
	    
	    return bMinVSumRDD;
	         

	  }


	  static JavaRDD<RaBin> createBins(JavaPairRDD<Integer, Integer> binStarCounts, 
			                           JavaPairRDD<Integer, BigDecimal> binColorIndexes,
			                           JavaPairRDD<Integer, RaBin> binStats) {
			
	         // Join to generate full Bin data
	         JavaPairRDD<Integer, Tuple2<Integer, BigDecimal>> joinRDD = binStarCounts.join(binColorIndexes);

	         // Produce an RDD of Bins
	         JavaPairRDD<Integer, RaBin> genBinRDD = joinRDD.mapToPair(f -> { RaBin temp = new RaBin(f._1(), f._2()._1(), f._2()._2());
	                                                           return new Tuple2<Integer, RaBin>(temp.getBinNum(), temp);});
	         
	  
      		 // Combine all Bin Data in RDD of Bins
	  		
	        JavaPairRDD<Integer, Tuple2<RaBin, RaBin>> lastRDD = genBinRDD.join(binStats);
	  		JavaRDD<RaBin> outputRDD = lastRDD.map(f -> { RaBin temp = new RaBin(f._1(), f._2()._1().getStarCount(), f._2()._1().getTotBMinV(),
	  				                                                         f._2()._2().getMedian(), f._2()._2().getMean(), f._2()._2().getStandD(),
	  				                                                         f._2()._2().getMinumum(), f._2()._2().getMaximum(), f._2()._2().getSpectralOcount(), 
	  				                                                         f._2()._2().getSpectralBcount(), f._2()._2().getSpectralAcount(), f._2()._2().getSpectralFcount(), 
	  				                                                         f._2()._2().getSpectralGcount(), f._2()._2().getSpectralKcount(), f._2()._2().getSpectralMcount());
	  		                                              return temp;}); 

		    
		    return outputRDD;
		         

		  }
  
	
	  static List<Double> collectStats(Integer bin, Iterable<BigDecimal> values) {
		
//		  StringBuffer output = new StringBuffer();
		  
		  Integer spectralOcount = 0; 
		  Integer spectralBcount = 0;
		  Integer spectralAcount = 0;
		  Integer spectralFcount = 0;
		  Integer spectralGcount = 0;
		  Integer spectralKcount = 0;
		  Integer spectralMcount = 0;
		  
		  Iterator i = values.iterator();
		  double[] entries = new double[2000];
		  int x = 0;
		  
		  while (i.hasNext())
		    {
		      BigDecimal temp = (BigDecimal) i.next();
		      entries[x] = temp.doubleValue();
		      
		      if (entries[x] >= 1.6) spectralMcount++;
			  else if (entries[x] >= 1.0) spectralKcount++;
			  else if (entries[x] >= .7) spectralGcount++;
			  else if (entries[x] >= .5) spectralFcount++;
			  else if (entries[x] >= .2) spectralAcount++;
			  else if (entries[x] >= -.2) spectralBcount++;
			  else if (entries[x] >= -.4) spectralOcount++;
		      
		      x++;
		    }
		  
		  double[] limitedEntries = new double[x];
		  
		  for (int y = 0; y < x; y++)
			  limitedEntries[y] = entries[y];
		  
//		  System.out.println("Limited Entries length: " + limitedEntries.length +" value of x: " +x);
		  
		  
		  DescriptiveStatistics stats = new DescriptiveStatistics(limitedEntries); 
		  double[] sorted = stats.getSortedValues();
		  double median;
		 
//		  System.out.println("Sorted length: " + sorted.length );
		  
		  if (sorted.length == 1) 
			  median = sorted[0];
		  else if (sorted.length % 2 == 1) {
//			  System.out.println(" got to odd");
		  	  median = sorted[((sorted.length)/2)];
//		  	  System.out.println(" median: " +median);
		  }	  
		  else {
//			  System.out.println(" got to even");
		  	  median = ((sorted[(sorted.length/2 -1)] + sorted[sorted.length/2])/2);
		  }	  
		  System.out.println(" RaBin : " + bin +" Items : " +sorted.length + "\n");
		  System.out.println(" RaBin : " + bin +" Mean : " + stats.getMean() +" Minimum : " + stats.getMin() 
				                               +" Maximum : " + stats.getMax() +" Median : " + median  
				                               +" Standard Deviation : " + stats.getStandardDeviation() + "\n");
		  
		  List<Double> statsData = new ArrayList<>(); 
		  statsData.add(new Double(stats.getN()));
		  statsData.add(stats.getMean());
		  statsData.add(median);
		  statsData.add(stats.getStandardDeviation());
		  statsData.add(stats.getMin());
		  statsData.add(stats.getMax());
		  statsData.add(new Double(spectralOcount));
		  statsData.add(new Double(spectralBcount));
		  statsData.add(new Double(spectralAcount));
		  statsData.add(new Double(spectralFcount));
		  statsData.add(new Double(spectralGcount));
		  statsData.add(new Double(spectralKcount));
		  statsData.add(new Double(spectralMcount));
		  
	      return statsData;	    

	  }  
	  
	static JavaPairRDD<Integer, RaBin> groupAndCollect(JavaPairRDD<Integer, BigDecimal> starColorIndexes) {
		
		JavaPairRDD<Integer, RaBin> binStats = starColorIndexes.groupByKey()
                .mapToPair(s -> {List<Double> output = collectStats(s._1(), s._2());
                                 RaBin temp = new RaBin();
                                 temp.setBinNum(s._1());
                                 temp.setStarCount(output.get(0).intValue());
                                 temp.setMean(new BigDecimal(output.get(1))); 
                                 temp.setMedian(new BigDecimal(output.get(2)));
                                 temp.setStandD(new BigDecimal(output.get(3)));
                                 temp.setMinumum(new BigDecimal(output.get(4)));
                                 temp.setMaximum(new BigDecimal(output.get(5)));
                                 temp.setSpectralOcount((output.get(6).intValue()));
                                 temp.setSpectralBcount((output.get(7).intValue()));
                                 temp.setSpectralAcount((output.get(8).intValue()));
                                 temp.setSpectralFcount((output.get(9).intValue()));
                                 temp.setSpectralGcount((output.get(10).intValue()));
                                 temp.setSpectralKcount((output.get(11).intValue()));
                                 temp.setSpectralMcount((output.get(12).intValue()));
                                 return new Tuple2<Integer, RaBin>(temp.getBinNum(), temp);
                                 });

		return binStats;
		
	}
	static void printMapCount(String title, Map<String, Long> countMap) {
	
	    System.out.println("\n" + title);
        for (Map.Entry<String, Long> entry : countMap.entrySet()) {
   	        System.out.println(" Key : " + entry.getKey() + " Count : " + entry.getValue());
   	
        }	    

	  }
	
	static void printBins(String title, Map<Integer, Long> countMap) {
		
	    System.out.println("\n" + title);
        for (Map.Entry<Integer, Long> entry : countMap.entrySet()) {
   	        System.out.println(" Key : " + entry.getKey() + " Count : " + entry.getValue());
   	
        }	    

	  }
}
