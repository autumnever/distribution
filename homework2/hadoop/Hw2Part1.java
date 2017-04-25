import java.io.IOException;
import java.util.StringTokenizer;
import java.text.DecimalFormat;
import java.text.NumberFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * zhangqiuping 201618013229025
 *
 * Hw2Grp1 <source> <destination> <count> <average time>
 *
 * @author zhangqiuping
 *
 * @version 1.0.0
 */
public class Hw2Part1 {

  // This is the Mapper class
  // reference: http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Mapper.html
  //
  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private Text mapoutvalue = new Text();  //time
    private Text word = new Text();  //source destination
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
  
      StringTokenizer itr = new StringTokenizer(value.toString(),"\n");
      while (itr.hasMoreTokens()) {
        String[] str=itr.nextToken().split(" ");  //every line segmentation with " "
        if(str.length == 3 && Double.parseDouble(str[2])>=0){
            String tmpkey = str[0] + " " + str[1];
            String tmpvalue = "1" + " " + str[2];
            word.set(tmpkey);
            mapoutvalue.set(tmpvalue);
            context.write(word, mapoutvalue);  //source destination 1 time
        }
      }      
    }
  }
  
  public static class IntSumCombiner
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int count = 0;
      double sumtime = 0;
      for (Text val : values) {
        count += Integer.parseInt(String.valueOf(val).split(" ")[0]);
        sumtime += Double.valueOf(String.valueOf(val).split(" ")[1]);
      }
      String tmpresult = String.valueOf(count) + " " + String.valueOf(sumtime);
      result.set(tmpresult);
      context.write(key, result);   //source destination count sumtime
    }
  }

  // This is the Reducer class
  // reference http://hadoop.apache.org/docs/r2.6.0/api/org/apache/hadoop/mapreduce/Reducer.html
  //
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {

    private Text result_key= new Text();
    private Text result_value= new Text();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int count = 0;
      double sumtime = 0;
      for (Text val : values) {
        count += Integer.parseInt(String.valueOf(val).split(" ")[0]);
        sumtime += Double.valueOf(String.valueOf(val).split(" ")[1]);
      }

      // generate result key
      result_key.set(key);

      // generate result value
      DecimalFormat df = new java.text.DecimalFormat("0.000");
      double tmp_ave_time = sumtime/count;
      String average_time = df.format(tmp_ave_time);
      String tmpresult_value = String.valueOf(count) + " " + average_time;
      result_value.set(tmpresult_value);

      context.write(result_key, result_value);  //source destination count average_time
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: source-destination <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "source-destination");

    job.setJarByClass(Hw2Part1.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumCombiner.class);
    job.setReducerClass(IntSumReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // add the input paths as given by command line
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }

    // add the output path as given by the command line
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
