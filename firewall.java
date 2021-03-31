import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class firewall {

	private final static int numOfReducers = 2;

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (args.length != 4) {
			System.err.println("Usage: WordCount <output>");
			System.exit(1);
		}

		SparkConf sparkConf = new SparkConf().setAppName("Firewall in Spark");
		// .setMaster("local[*]");
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		JavaRDD<String> ip_trace = context.textFile(args[0]);
		JavaRDD<String> raw_block = context.textFile(args[1]);

//		JavaRDD<String> words = raw_block_lines.map(new FlatMapFunction<String, String>() {
//			public Iterator<String> call(String s) {
//				return Arrays.asList(s.split("\\s+")).iterator();
//			}
//		});

		// JavaPairRDD<key_type, value_type> var2 = var1.mapToPair(new
		// PairFunction<input_type, key_type, value_type>() {
		// public Tuple2<key_type, value_type> call(input_type s) {
		// ......
		// return new Tuple2<key_type, value_type>(..., ...);
		// }
		// });

		JavaPairRDD<Integer, String> ip_trace_kv = ip_trace.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) {

				String time = s.split("\\s+")[0];
				String connectionID = s.split("\\s+", 7)[1];
				String sourceIP = s.split("\\s+", 7)[2];
				String destIP = s.split("\\s+", 7)[4];
//				String protocol = s.split("\\s+", 7)[5];

				String trimmedSourceIP = sourceIP.split("\\.")[0] + "." + sourceIP.split("\\.")[1] + "."
						+ sourceIP.split("\\.")[2] + "." + sourceIP.split("\\.")[3];

				String trimmedDestIP = destIP.split("\\.")[0] + "." + destIP.split("\\.")[1] + "."
						+ destIP.split("\\.")[2] + "." + destIP.split("\\.")[3];

				return new Tuple2<Integer, String>(Integer.parseInt(connectionID),
						time + " " + trimmedSourceIP + " " + trimmedDestIP);
			}
		});

		JavaPairRDD<Integer, String> raw_block_kv = raw_block.mapToPair(new PairFunction<String, Integer, String>() {
			public Tuple2<Integer, String> call(String s) {
				String actionTaken = s.split("\\s+")[1];
				return new Tuple2<Integer, String>(Integer.parseInt(s.split("\\s+")[0]), actionTaken);
			}
		});

		Function<Tuple2<Integer, String>, Boolean> blockFilter = t -> (t._2.equals("Blocked"));

		JavaPairRDD<Integer, String> filtered_raw_block_kv = raw_block_kv.filter(blockFilter);

		// JavaPairRDD<key_type, output_type> var2 = var1.reduceByKey(new
		// Function2<value_type, value_type, output_type>() {
		// public output_type call(value_type i1, value_type i2) {
		// return ......
		// }
		// }, numOfReducers);
		JavaPairRDD<Integer, Tuple2<String, String>> join = ip_trace_kv.join(filtered_raw_block_kv).sortByKey();

		JavaRDD<String> firewallLog = join.map(new Function<Tuple2<Integer, Tuple2<String, String>>, String>() {
			public String call(Tuple2<Integer, Tuple2<String, String>> t) {
				String time = t._2._1.split("\\s+")[0];
				String connectionID = String.valueOf(t._1);
				String sourceIP = t._2._1.split("\\s+")[1];
				String destIP = t._2._1.split("\\s+")[2];
				String blocked = t._2._2;

				return time + " " + connectionID + " " + sourceIP + " " + destIP + " " + blocked;
			}
		});

		// JavaPairRDD<key_type, value_type> var2 = var1.mapToPair(new
		// PairFunction<input_type, key_type, value_type>() {
		// public Tuple2<key_type, value_type> call(input_type s) {
		// ......
		// return new Tuple2<key_type, value_type>(..., ...);
		// }
		// });
		JavaPairRDD<String, Integer> ones = firewallLog.mapToPair(new PairFunction<String, String, Integer>() {
			public Tuple2<String, Integer> call(String s) {

				String sourceIP = s.split("\\s+")[2];

				return new Tuple2<String, Integer>(sourceIP, 1);
			}
		});

		// JavaPairRDD<key_type, output_type> var2 = var1.reduceByKey(new
		// Function2<value_type, value_type, output_type>() {
		// public output_type call(value_type i1, value_type i2) {
		// return ......
		// }
		// }, numOfReducers);
		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		}, numOfReducers);

		JavaPairRDD<Integer, String> swap = counts
				.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
					public Tuple2<Integer, String> call(Tuple2<String, Integer> t) {
						return t.swap();
					}
				});

		JavaPairRDD<Integer, String> sorted = swap.sortByKey(false);

		firewallLog.saveAsTextFile(args[2]);
		sorted.saveAsTextFile(args[3]);
		context.stop();
		context.close();

	}

}
