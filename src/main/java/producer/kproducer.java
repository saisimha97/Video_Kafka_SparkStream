package producer;
import java.sql.Timestamp;
import java.util.Base64;

import java.util.Properties;


import org.apache.kafka.clients.producer.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;


import org.apache.kafka.clients.producer.ProducerRecord;

import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.Size;
import org.opencv.imgproc.Imgproc;
import org.opencv.videoio.VideoCapture;
import org.opencv.highgui.Highgui;
import org.opencv.objdetect.CascadeClassifier;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class kproducer {
	
	static {
		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);
	}

	
	// Adaboost (Decision Tree model => Stump)
	private static String AdaBoostName = "haarcascade_frontalface_alt.xml";
	
	private static CascadeClassifier AdaBoost = new CascadeClassifier();

	private static Gson gson;
	
	

	public static void main(String[] args) throws Exception{


// all producer and kafka broker config
		
		gson = new Gson();
		String topicName = "jbm";
        Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");

		      
		props.put("acks", "all");

		
		props.put("retries", 1);

		
		props.put("batch.size", 20971270);

		 
		props.put("linger.ms", 3);

   
		props.put("buffer.memory", 33554432);

		props.put("max.request.size", 2097152);
		
		props.put("compression.type", "gzip");

		props.put("key.serializer", 
				"org.apache.kafka.common.serialization.StringSerializer");

		props.put("value.serializer", 
				"org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<String, String>(props);

	//streaming face detected video
		getDetected(producer); 

	}

	static void getDetected(Producer<String, String> producer)
	{

		VideoCapture capture;
		Mat frame = new Mat();

		
		if (!AdaBoost.load(AdaBoostName))
		{
			System.out.print("Could not load AdaBoost model\n");

		}
		capture = new VideoCapture(0);

		if(!capture.isOpened())

			System.out.println("Did not connect to camera.");


		Mat frame2;

		while(capture.read(frame))
		{



			frame2 =   detect(frame);

			int cols = frame2.cols();
			int type = frame2.type();
			int rows = frame2.rows();

			byte[] data = new byte[(int) (frame2.total() * frame2.channels())];
			frame2.get(0, 0, data);
			String timestamp = new Timestamp(System.currentTimeMillis()).toString();

			JsonObject obj = new JsonObject();
	
			
			// for multiple video camera just add camera ID as another parameter
			obj.addProperty("timestamp", timestamp);
			obj.addProperty("rows", rows);
			obj.addProperty("cols", cols);
			obj.addProperty("type", type);
			obj.addProperty("data", Base64.getEncoder().encodeToString(data));  
			String json = gson.toJson(obj);
			producer.send(new ProducerRecord<String, String>("jbm","webcam", json));

		}
		
		return;
	}


	



public static Mat detect(Mat frame)
{
	Mat frame_gray = new Mat();
	MatOfRect face = new MatOfRect();

	Rect[] facesArray = face.toArray();

	Imgproc.cvtColor(frame, frame_gray, Imgproc.COLOR_BGRA2GRAY);
	Imgproc.equalizeHist(frame_gray, frame_gray);


	AdaBoost.detectMultiScale( frame_gray, face, 1.1, 2, 0, new Size(30, 30), new Size() );

	for (int i = 0; i < facesArray.length; i++)
	{

		Point center = new Point(facesArray[i].x + facesArray[i].width * 0.5, facesArray[i].y + facesArray[i].height * 0.5);
		Core.ellipse(frame, center, new Size(facesArray[i].width * 0.5, facesArray[i].height * 0.5), 0, 0, 360, new Scalar(255, 0, 255), 4, 8, 0);

		Mat faceROI = frame_gray.submat(facesArray[i]);
	}

	return frame;

}

}
