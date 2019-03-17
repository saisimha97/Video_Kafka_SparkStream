package sparkStream;


// this is the main consumer
import java.util.Base64;
import java.util.Scanner;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.opencv.imgcodecs.Imgcodecs;
import org.opencv.core.Mat;



public class Stream2 {

static int partitions;

static {
	System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
}

	public static void main(String[] args) throws Exception {
		


		
		Scanner scan = new Scanner(System.in);
		System.out.println("Enter the number of partitions required");
		partitions=scan.nextInt();
		
		scan.close();
		
		SparkSession spark = SparkSession
				.builder()
				.appName("VideoStreamProcessor")
				.master("local")
				.getOrCreate(); 

		StructType schema =  DataTypes.createStructType(new StructField[] { 

				DataTypes.createStructField("timestamp", DataTypes.TimestampType, true),
				DataTypes.createStructField("rows", DataTypes.IntegerType, true),
				DataTypes.createStructField("cols", DataTypes.IntegerType, true),
				DataTypes.createStructField("type", DataTypes.IntegerType, true),
				DataTypes.createStructField("data", DataTypes.StringType, true)
		});

		
		//whole kafka consumer config
		Dataset<SnapStream> ds = spark.readStream().format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "jbm")
				.option("kafka.max.partition.fetch.bytes", 2097152)
				.option("kafka.max.poll.records", 350).load()
				.selectExpr("CAST(value AS STRING) as message")
				.select(functions.from_json(functions.col("message"),schema).as("json"))
				.select("json.*").as(Encoders.bean(SnapStream.class)); 
		
		ds.repartition(partitions);


		ds.foreach(values -> {

			SnapStream encoded;
			Mat image ;
			encoded = values;

			image = new Mat(encoded.getRows(), encoded.getCols(), encoded.getType());
			image.put(0, 0, Base64.getDecoder().decode(encoded.getData()));

			String imagePath = "/home/sai"+"WebCam-"+encoded.getTimestamp().getTime()+".png";

			Imgcodecs.imwrite(imagePath, image);



		});
		
		// starting stream
		StreamingQuery query = ds.writeStream().outputMode("update").format("console").start();

	
		query.awaitTermination();
		
		
	





	}



}

