import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import java.util.List;
import java.util.Arrays;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Lab2{

  public static class KmerCountMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    List<Character> Alphabet= Arrays.asList(new Character[]{'A','C','G','T'});
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      Configuration conf = context.getConfiguration();

      int k = Integer.parseInt(conf.get("k"));
     
      String input = value.toString();

      // If line does not start with A,C,G, or T
      // it's the header or bogus line => ignore it
      if (Alphabet.contains(input.charAt(0))){
          int subStringNb = (input.length() - k) + 1;

          for (int i = 0; i < subStringNb; i++){
	     String kmer = input.substring(i,k+i);
	
	     word.set(kmer);
	     context.write(word, one);
          }
      } 
    }
  }

  public static class KmerCountReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("k", args[0]);

    Job job = Job.getInstance(conf, "k-mer count");
    job.setJarByClass(Lab2.class);
    job.setMapperClass(KmerCountMapper.class);
    job.setCombinerClass(KmerCountReducer.class);
    job.setReducerClass(KmerCountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
