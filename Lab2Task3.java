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

public class Lab2Task3 {

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
        private Map<Long, Text> countMap = new HashMap<>();

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String start = null;// current line
            String end = null;// start of next line
            for (Text val : values) {
                String str = val.toString();
                if (str.charAt(0) == '<') end = str;
                else start = str;
            }
            if (start != null && end != null) {
                String result = start.substring(0, start.length() - 1) + end.substring(1, end.length());
                countMap.put(key.get(), new Text(result));
            } else if (start != null && end == null) {// if no next line don't try to append
                String result = start.substring(0, start.length() - 1);
                countMap.put(key.get(), new Text(result));
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<Long, Text>> sortedList = countMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList());

            int lineId = 0;
            for (Map.Entry<Long, Text> entry : sortedList) {
                context.write(new LongWritable(lineId++), entry.getValue());
            }
        }
    }

    public static class KmerCountMapper extends Mapper<Object, Text, Text, IntWritable>{
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));
            String[] values = value.toString().split("\t");
            int offset = Integer.parseInt(values[0]);
            String input = values[1];
            int subStringNb = (input.length() - k) + 1;

            for (int i = 0; i < subStringNb; i++){
                String kmer = input.substring(i,k+i);

                int pos = subStringNb * offset + i;
                word.set(kmer);
                context.write(word, new IntWritable(pos));
            }
        }
    }

    public static class KmerCountReducer extends Reducer<Text,IntWritable,IntWritable,Text> {
        private Map<Integer, Text> countMap = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int pos = -1;
            for (IntWritable val : values) {
                if (pos != -1) return;// make sure the k-mer is unique
                pos = val.get();
            }
            countMap.put(pos, new Text(key.toString()));
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int k = Integer.parseInt(conf.get("k"));

            List<Map.Entry<Integer, Text>> sortedList = countMap.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toList());

            int counter = 0;
            int previous = -1;
            int startOfCurrent = -1;
            int startOfBest = -1;
            int counterOfBest = -1;
            for (Map.Entry<Integer, Text> entry : sortedList) {
                int current = entry.getKey();
                if (current - previous == 1) {
                    counter++;
                } else {// if end of chain
                    if (startOfBest == -1) {// if not initialized
                        startOfBest = current;
                        counterOfBest = counter;
                    } else if (counter > counterOfBest) {// if initialized and better (bigger)
                        startOfBest = startOfCurrent;
                        counterOfBest = counter;
                    }
                    // reset
                    counter = 0;
                    startOfCurrent = current;
                }
                previous = current;
            }
            
            StringBuilder sb = new StringBuilder(countMap.get(startOfBest).toString());
            for (int i = 1; i <= counterOfBest; i++) {
                sb.append((char)(countMap.get(startOfBest + i).charAt(k - 1)));
            }
            context.write(new IntWritable(counterOfBest + k), new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("k", args[0]);

        Job job1 = Job.getInstance(conf, "line combiner");
        job1.setJarByClass(Lab2Task3.class);
        job1.setMapperClass(LineCombinerMapper.class);
        job1.setReducerClass(LineCombinerReducer.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path("temp"));

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "k-mer count");
        job2.setJarByClass(Lab2Task3.class);
        job2.setMapperClass(KmerCountMapper.class);
        job2.setReducerClass(KmerCountReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("temp"));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
