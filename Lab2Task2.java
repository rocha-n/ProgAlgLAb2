import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Lab2Task2 {

    public static class LineCombinerMapper extends Mapper<Object, Text, LongWritable, Text>{
        private Text word = new Text();
        List<Character> Alphabet= Arrays.asList(new Character[]{'A','C','G','T'});

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));
            String input = value.toString();
            // If line does not start with A,C,G, or T
            // it's the header or bogus line => ignore it
            if (Alphabet.contains(input.charAt(0))) {
                LongWritable start = new LongWritable(((LongWritable) key).get());// key for previous line
                LongWritable end = new LongWritable(start.get() + input.length() + 1);// key for current line

                word.set("<" + input.substring(0, Math.min(k - 1, input.length())));// give k-1 first letters to previous line
                context.write(start, word);

                word.set(input + ">");
                context.write(end, word);
            }
        }
    }

    public static class LineCombinerReducer extends Reducer<LongWritable,Text,LongWritable,Text> {
        private Text result = new Text();

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String start = null;// current line
            String end = null;// start of next line
            for (Text val : values) {
                String str = val.toString();
                if (str.charAt(0) == '<') end = str;
                else start = str;
            }
            if (start != null && end != null) {
                result.set(start.substring(0, start.length() - 1) + end.substring(1, end.length()));
                context.write(key, result);
            } else if (start != null && end == null) {// if no next line don't try to append
                result.set(start.substring(0, start.length() - 1));
                context.write(key, result);
            }
        }
    }

    public static class KmerCountMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));
            String input = value.toString().split("\t")[1];// drop the key
            int subStringNb = (input.length() - k) + 1;

            for (int i = 0; i < subStringNb; i++){
                String kmer = input.substring(i,k+i);

                word.set(kmer);
                context.write(word, one);
            }
        }
    }

    public static class KmerCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private Map<Text, Integer> countMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(new Text(key), sum);
        }

        // code taken from https://github.com/andreaiacono/MapReduce/blob/master/src/main/java/samples/topn/TopN.java
        public void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("n"));

            List<Map.Entry<Text, Integer>> sortedList = countMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.<Text, Integer>comparingByValue().reversed())
                    .collect(Collectors.toList());

            int counter = 0;
            for (Map.Entry<Text, Integer> entry : sortedList) {
                context.write(entry.getKey(), new IntWritable(entry.getValue()));
                if (++counter >= n) break;
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("k", args[0]);
        conf.set("n", args[1]);

        Job job1 = Job.getInstance(conf, "line combiner");
        job1.setJarByClass(Lab2Task2.class);
        job1.setMapperClass(LineCombinerMapper.class);
        job1.setReducerClass(LineCombinerReducer.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path("temp"));

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "k-mer count");
        job2.setJarByClass(Lab2Task2.class);
        job2.setMapperClass(KmerCountMapper.class);
        job2.setReducerClass(KmerCountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
