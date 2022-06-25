import java.io.*;
import java.math.*;
import org.apache.hadoop.io.WritableComparator;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.WritableComparable;

public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text one = new Text();
    private Text word = new Text();
    private int var2 = 0;
    private int k = 0;
    private double mo = 0;
    private String val = null;
    private String [] var3 = new String[2239];
    private String [] var4 = null;
    int l = 0;
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     
    String [] var = (value.toString().split(";"));
    
             int j = 0;
             int vl = 0;
             int i=0;
	     var2 += Integer.parseInt(var[9]);
	    
	     
	   
	     var3[k] = var[9] + ";" +var[0] +";"+ var[1] +";"+ var[2] +";"+ var[3] +";"+ var[4] +";"+ var[9];	
	     
	     k+=1;           		
  

              
               if(k==2239){
                  mo = var2/k;
                  for(i=0;i<k;i++){
                    String [] var4 = var3[i].split(";");
                    vl = Integer.parseInt(var4[6]);
                     if(vl > mo){
                         if(vl >=900)
                         {
                           String [] ar = var3[i].split(";");
                           int ar1 = Integer.parseInt(var4[0])+90000;
                           ar[0] = Integer.toString(ar1);
                           val = ar[0]+ ";"+var3[i];
                           word.set(val);
                           context.write(word, one);
                         }
                         else if(vl >=1000)
                         {
                           String [] ar = var3[i].split(";");
                           int ar1 = Integer.parseInt(var4[0])+99000;
                           ar[0] = Integer.toString(ar1);
                           val = ar[0]+";" +var3[i];
                           word.set(val);
                           context.write(word, one);
                         }
                         else{
                         val = var4[0]+";"+var3[i];
		           word.set(val);
                           context.write(word, one);}
		         
		         }
                     } 
                     
                   }
    
    }
  }
  
  


  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,Text> {
    private Text result = new Text();
    private int count = 1;
    private String [] tel = new String[8];
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      
     
       Text word = new Text();
       tel =  key.toString().split(";");
        String vare = tel[2]+" "+tel[3]+" "+tel[4]+" "+tel[5]+" "+tel[6]+" "+tel[7];
        key.set(vare);
        result.set(Integer.toString(count));
        context.write(result,key);
        count += 1;  
     
      
     
   
                
      
        }
      
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
   // job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
