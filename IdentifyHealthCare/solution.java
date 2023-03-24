package com.mr;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class IdentifyHealCare {
	//public static Logger logger = Logger.getLogger(IdentifyHealCare.class.getName());
	public static Integer[] encryptCol={2,3,4,5,6,8};
	private static byte[] key1 = new String("samplekey1234567").getBytes();

	public static class Map extends Mapper<Object, Text, NullWritable, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//value = PatientID, Name,DOB,Phone Number,Email_Address,SSN,Gender,Disease,weight
			StringTokenizer itr = new StringTokenizer(value.toString(),",");
			List<Integer> list=new ArrayList<Integer>();
			Collections.addAll(list, encryptCol);
			//list=2,3,4,5,6,8
			//System.out.println("Mapper :: one :"+value);
			String newStr="";
			int counter=1;
			while (itr.hasMoreTokens()) {
				String token=itr.nextToken();
				//System.out.println("token"+token);
				//System.out.println("i="+counter);
				if(list.contains(counter)){
					if(newStr.length()>0) {newStr+=",";}
					newStr+=encrypt(token, key1);
				} else {
					if(newStr.length()>0) {newStr+=",";}
					newStr+=token;
				}
				counter=counter+1;
			}
			context.write(NullWritable.get(), new Text(newStr.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(Map.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setJarByClass(IdentifyHealCare.class);
		job.waitForCompletion(true);
	}

	public static String encrypt(String strToEncrypt, byte[] key){
		try {
			Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
			SecretKeySpec secretKey = new SecretKeySpec(key, "AES");
			cipher.init(Cipher.ENCRYPT_MODE, secretKey);
			//cipher.init(Cipher.DECRYPT_MODE, secretKey);
			String encryptedString = Base64.encodeBase64String(cipher.doFinal(strToEncrypt.getBytes()));
			//String decrypted = new String(cipher.doFinal(Base64.decodeBase64(strToEncrypt)));
			return encryptedString.trim();
			//return decrypted;
		} catch (Exception e) {
			//logger.error("Error while encrypting", e);
			System.out.println("Error while encrupting");
		} return null;
	}
}
