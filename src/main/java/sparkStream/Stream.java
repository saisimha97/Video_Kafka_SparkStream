package sparkStream;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;
//import org.apache.spark.streaming.kafka.KafkaUtils;
import org.opencv.core.Mat;

import org.opencv.imgcodecs.Imgcodecs;

// implement from Apache Spark user docs

public class Stream


{
	
	static {
		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
	}
	
	static int partitions;
	
	
	public static void main(String[] agrs ) throws InterruptedException {
Map<String, Object> kafkaParams = new HashMap<String, Object>();


kafkaParams.put("bootstrap.servers", "localhost:9092");
kafkaParams.put("max.partition.fetch.bytes", 2097152);
kafkaParams.put("max.poll.records", 350);
kafkaParams.put("key.deserializer", StringDeserializer.class);
kafkaParams.put("value.deserializer", StringDeserializer.class);
kafkaParams.put("group.id", "test");
kafkaParams.put("auto.offset.reset", "latest");
kafkaParams.put("enable.auto.commit", false);

Collection<String> topics = Arrays.asList("jbm");


SparkConf conf = new SparkConf().setAppName("KafkaInput");
// 1 sec batch size
JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(1));



final JavaInputDStream<ConsumerRecord<String, String>> stream =
  KafkaUtils.createDirectStream(
    jssc,
    LocationStrategies.PreferConsistent(),
    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
  );

JavaDStream<String> lines = stream.map(ConsumerRecord::value);

Scanner scan = new Scanner(System.in);
System.out.println("Enter the number of partitions required");
partitions=scan.nextInt();

scan.close();

lines.foreachRDD(new VoidFunction<JavaRDD<String>>() {
    /**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
    public void call(JavaRDD<String> rdd) { 
        JavaRDD<String> rowRDD = rdd.map(new Function<String, String>() {
            

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
            public String call(String msg) {
             
            	Encoder<SnapStream> encoder = Encoders.bean(SnapStream.class);
            	SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
            	
            	 Dataset<SnapStream> ds = spark.read().json(msg).as(encoder);
            	 
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
            	
            	
              return msg;
            }
            
            
          });

       

    }
});


jssc.start();

jssc.awaitTermination();

}
}
class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
      if (instance == null) {
        instance = SparkSession
          .builder()
          .config(sparkConf)
          .getOrCreate();
      }
      return instance;
    }
  }
