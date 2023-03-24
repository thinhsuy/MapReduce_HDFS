import java.io.*;
import java.util.*;

import org.apache.commons.codec.binary.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IdentifyHealCare {
  public static Integer[] encryptCol = {2,3,4,5,6,8};
  private static byte[] keyEncrypt = new String("samplekey1234567").getBytes();

  public static String encrypt(String strToEncrypt, byte[] key) {
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
        //return null;
     } return null;
  }
  //									 >>change this for another type of value class output
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //split current line into list of values by removing consecutive spaces
      String[] harsh = (value.toString()).split(",");
      ArrayList<Integer> list = new ArrayList<Integer>();
      Collections.addAll(list, encryptCol);
      String encryptedString = "";
      int counter = 1;
      for (int i=0; i<harsh.length; i++){
          if (list.contains(counter)){
              if (encryptedString.length()>0) {encryptedString+=",";}
              encryptedString+=encrypt(harsh[i].toString(), keyEncrypt);
          } else {
              if  (encryptedString.length()>0) {encryptedString+=",";}
              encryptedString+=harsh[i].toString();
          }
          counter+=1;
      }
      context.write(new Text("key"), new Text(encryptedString));
    }
  }
  //									    >> change this for another type of value class output
  public static class TokenReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      for (Text val : values){
          context.write(new Text("encrypted"), new Text(val.toString()));
      }
      //context.write(NullWritable.get(), new Text(values.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Identify Healcare Program");
    job.setJarByClass(IdentifyHealCare.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(TokenReducer.class);
    job.setReducerClass(TokenReducer.class);

    //setting new output format for Mapper and Reducer
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
