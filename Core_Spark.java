package assignmentspark;
//importing the libraries required

import java.util.Arrays;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {

	public static void main(String[] args) {
		Logger.getLogger("org.apache").setLevel(Level.WARN);
		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		//SparkConf conf = new SparkConf().setAppName("startingSpark");
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 1. Read the data from file reviews.txt and store into a Java RDD in Spark.
		// Print the count of records in the RDD.
		// Also print the number of partitions present in the RDD.

		JavaRDD<String> initialRdd = sc.textFile("src/resources/reviews.txt");
//		JavaRDD<String> initialRdd = sc.textFile("/user/bigdata/reviews.txt");
		//initialRdd.foreach(sentence -> System.out.println(sentence));

		// finding count of records and number of partitions 
		long counts = initialRdd.count();
		System.out.println("\nCount of Records in File is " + counts);

		int no_of_partitions = initialRdd.getNumPartitions();
		System.out.println("\nNumber of Partitions is " + no_of_partitions);

		// 2. Remove all of the following special characters from the records
		
		JavaRDD<String> cleanedRdd = initialRdd.map(sentence -> sentence.replace("'", "")
				.replaceAll("[^a-zA-Z0-9]", " ").trim().replaceAll("br    br   ", "").replaceAll("\\s+"," " ).toLowerCase()).cache();
	
		cleanedRdd.foreach(sentence -> System.out.println(sentence));
		long cleanedcount = cleanedRdd.count();
		// System.out.println(cleanedcount);

		// 3. Print the number of records that do not contain any numeric character.
		JavaRDD<String> withoutnumericRdd = cleanedRdd.filter(sentence -> !sentence.matches(".*\\d.*"));
		long result = withoutnumericRdd.count();
		System.out.println("\nNumber of records without numeric is " + result);

		// 4. Print total count of the occurrence of word ‘movie’ in all records.
		JavaRDD<String> moviewords = cleanedRdd.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator())
				.filter(word -> word.equals("movie"));
		System.out.println("\nCount of the occurrence of word ‘movie’ is " + moviewords.count());

		// 5. Print the minimum and the maximum length of the review.
		Tuple2<Long, String> minlineCount = cleanedRdd
				.mapToPair(line -> new Tuple2<Long, String>((long) line.length(), line)).sortByKey(true).first();
		System.out.println("\nMinimum length of review is " + minlineCount._1());

		Tuple2<Long, String> maxlineCount = cleanedRdd
				.mapToPair(line -> new Tuple2<Long, String>((long) line.length(), line)).sortByKey(false).first();
		System.out.println("\nMaximum length of review is " + maxlineCount._1());

		// 6. Write the data of cleansed records from task 2 in a file named
		// reviews_cleansed.txt.
		//cleanedRdd.coalesce(1).saveAsTextFile("C:\\Users\\Tushar_Dalvi\\Desktop\\cleanedData\\reviews_cleansed.txt.");

		// 7. Note down the Turn Around Time (TAT) for each of the above operations.
/*
Job Id			 Description		      Duration	
8			first at Main.java:66      		1.0 s
7			sortByKey at Main.java:66		24 ms
6			first at Main.java:62			1.0 s	
5			sortByKey at Main.java:62		37 ms
4			count at Main.java:48			0.7 s
3			count at Main.java:52			1s		
2			count at Main.java:47			23 ms	
1			for each at Main.java:46		7 s
0			count at Main.java:36			0.4 s	

*/
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();

		sc.close();
	}
}
