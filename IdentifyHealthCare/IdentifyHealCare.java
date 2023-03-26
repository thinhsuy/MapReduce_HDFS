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
  public static Integer[] encryptCol = {1,2,3,4,5,7};
  private static int keyEncrypt = 20127333;
  public static final String CHARACTERS = "abcdefghijklmnopqrstuvwxyz0123456789";
  private static boolean isHeaderLine = true;

  public static String Ceasar_encrypt(String inputStr, int shiftKey){
     // convert inputStr into lower case
     inputStr = inputStr.toLowerCase();
     // encryptStr to store encrypted data
     String encryptStr = "";
     // use for loop for traversing each character of the input string
     for (int i = 0; i < inputStr.length(); i++){
        // get position of each character of inputStr in ALPHABET
        int pos = CHARACTERS.indexOf(inputStr.charAt(i));
        // get encrypted char for each char of inputStr
        int encryptPos = (shiftKey*i + pos) % CHARACTERS.length();
        char encryptChar = CHARACTERS.charAt(encryptPos);
        // add encrypted char to encrypted string
        encryptStr += encryptChar;
     }
     return encryptStr;
  }

  public static String Ceasar_descrypt(String inputStr, int shiftKey){
        // convert inputStr into lower case
        inputStr = inputStr.toLowerCase();
        // decryptStr to store decrypted data
        String decryptStr = "";
        // use for loop for traversing each character of the input string
        for (int i = 0; i < inputStr.length(); i++) {
            // get position of each character of inputStr in ALPHABET
            int pos = CHARACTERS.indexOf(inputStr.charAt(i));
            // get decrypted char for each char of inputStr
            int decryptPos = (pos - shiftKey*i) % CHARACTERS.length();
            // if decryptPos is negative
            if (decryptPos < 0){
                decryptPos = CHARACTERS.length() + decryptPos;
            }
            char decryptChar = CHARACTERS.charAt(decryptPos);
            // add decrypted char to decrypted string
            decryptStr += decryptChar;
        }
        return decryptStr;
   }

  //						    <inputKey, inputValue, outputKey, outputValue>
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      if (isHeaderLine) { // Check for header line
         isHeaderLine = !isHeaderLine;
         context.write(new Text("header"), new Text(value.toString()));
         return;
      }
      //split current line into list of values by removing consecutive spaces
      String[] harsh = (value.toString()).split(",");
      //Create an array list to utilize several built function on this libary
      ArrayList<Integer> listEncryptCol = new ArrayList<Integer>();
      //Assign all value in encryptCol (static array) to listEncryptCol (dynamic array)
      Collections.addAll(listEncryptCol, encryptCol);
      String encryptedString = "";
      for (int index=0; index<harsh.length; index++){
          // encode index was defined as encryptable
          if (listEncryptCol.contains(index)){
              encryptedString += Ceasar_encrypt(harsh[index].toString(), keyEncrypt);
          } else {
              encryptedString += harsh[index].toString();
          }
          // add ',' for the value is not end of the line
          encryptedString += index==harsh.length-1 ? "" : ",";
      }
      context.write(new Text("encrypted"), new Text(encryptedString));
    }
  }
  //									    >> change this for another type of value class output
  public static class TokenReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // write all each values
      for (Text val : values){
         context.write(key, new Text(val.toString()));
      }
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
