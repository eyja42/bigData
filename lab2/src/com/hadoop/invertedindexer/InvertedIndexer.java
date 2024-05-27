package com.hadoop.invertedindexer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexer {
    public static class InvertedIndexMapper extends Mapper <LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        // default RecordReader : LineRecordReader ; key: line offset; value: line string
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            Text word = new Text();

            // Remove the chars neither letters nor digits
            String value_str = value.toString().toLowerCase();
            StringBuilder sBuilder = new StringBuilder();
            for (int i = 0; i < value_str.length(); ++i){
                if (value_str.charAt(i) >= 'a' && value_str.charAt(i) <= 'z' || value_str.charAt(i) >= '0' && value_str.charAt(i) <= '9'){
                    sBuilder.append(value_str.charAt(i));
                }
                else if (i != 0 && value_str.charAt(i) == '\'' && (value_str.charAt(i-1) >= 'a' && value_str.charAt(i-1) <= 'z')){
                    sBuilder.append(value_str.charAt(i));
                }
                else{
                    sBuilder.append(" ");
                }
            }
            value_str = sBuilder.toString().trim(); // remove the blank at the front and back

            // Split the words by space
            StringTokenizer itr = new StringTokenizer(value_str);
            while(itr.hasMoreTokens())
            {
                word.set(itr.nextToken());
                context.write(new Text(word.toString() + "#" + fileName), new Text(key.toString()));
            }
        }
    }

    public static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        // sum the count of a word in a file
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            int cnt = 0;
            for (Text v : values){
                cnt++;
            }
            context.write(key, new Text(String.valueOf(cnt)));
        }
    }

    public static class InvertedIndexPartitioner extends HashPartitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text values, int numReduceTasks)
        {
            String word = key.toString().split("#")[0];
            return super.getPartition(new Text(word), values, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        private String curr_word = "";
        private StringBuilder posting = new StringBuilder();    // where the word from and how many the word is
        private int word_sum = 0;   // the sum of words
        private int doc_sum = 0;    // the sum of docs
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            String word = key.toString().split("#")[0];
            String doc = key.toString().split("#")[1];
            if (!curr_word.equals(word) && !curr_word.isEmpty()){
                context.write(new Text(curr_word), new Text(value()));
            }
            int sum = 0;
            for (Text v : values) {
                sum += Integer.parseInt(v.toString());
            }
            word_sum += sum;
            doc_sum += 1;
            posting.append(doc).append(":").append(sum).append(";");
            curr_word = word;
        }

        protected String value()
        {
            double avg = 1.0 * word_sum / doc_sum;  // the average
            String value = String.format("%.2f", avg) + "," + posting.toString();
            word_sum = doc_sum = 0;
            posting = new StringBuilder();
            return value;
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException
        {
            context.write(new Text(curr_word), new Text(value()));
        }
    }

    public static class SortMapper extends Mapper <LongWritable, Text, DoubleWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        // default RecordReader : LineRecordReader ; key: line offset; value: line string
        {
            String LineRecord = value.toString().substring(0, value.toString().indexOf(","));
            Text word = new Text(LineRecord.split("\t")[0]);
            DoubleWritable avg = new DoubleWritable(Double.parseDouble(LineRecord.split("\t")[1]));
            context.write(avg, word);
        }
    }

    public static class SortDecreasingComparator extends DoubleWritable.Comparator {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class SortReducer extends Reducer <DoubleWritable, Text, Text, Text> {
        private int count = 0;
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException
        {
            Text avg = new Text(String.format("%.2f", Double.parseDouble(key.toString())));
            for (Text word : value){
                if (count < 50) {
                    count++;
                    context.write(avg, word);
                }
            }
        }

    }

    public static class TFIDFMapper extends Mapper <LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException
        // default RecordReader : LineRecordReader ; key: line offset; value: line string
        {
            String LineRecord = value.toString();
            String word = LineRecord.substring(0, LineRecord.indexOf("\t"));
            String[] files = LineRecord.substring(LineRecord.indexOf(",") + 1).split(";");
            for (String file : files){
                if (!file.isEmpty()){
                    context.write(new Text(word), new Text(file));
                }
            }
        }
    }

    public static String input_address = "/data/exp2";
    public static class TFIDFReducer extends Reducer <Text, Text, Text, Text> {
        private int file_count = 40;
        public void setup(Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem hdfs = FileSystem.get(conf);
            FileStatus[] p = hdfs.listStatus(new Path(input_address));        // to get the file count
            file_count = p.length;
        }

        @Override
        public void reduce(Text key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            String word = key.toString();
            int sum = 0;
            List<String> value_lst = new ArrayList<>();
            for (Text v : value) {
                sum += 1;
                value_lst.add(v.toString());
            }
            double idf = Math.log10(1.0 * file_count / (sum + 1)); // get the IDF!
            for (String v : value_lst) {
                String filename = v.split(":")[0];
                int tf = Integer.parseInt(v.split(":")[1]);
                BigDecimal tf_idf = new BigDecimal(tf).multiply(new BigDecimal(idf));
                context.write(new Text(filename + "," + word + "," + tf_idf.setScale(4, RoundingMode.HALF_UP)), new Text());
            }
        }
    }

    public static void main(String[]args) throws Exception{
        if(args.length < 2) {
            System.err.println("Usage: Relation <in> <out1> (<out2> <out3>)");
            System.exit(2);
        }
        Configuration conf = new Configuration();
        input_address = args[0];

        // JOB1：倒排索引
        Job job1 = Job.getInstance(conf, "InvertedIndex");
        job1.setJarByClass(InvertedIndexer.class);
        job1.setMapperClass(InvertedIndexMapper.class);
        job1.setCombinerClass(InvertedIndexCombiner.class);
        job1.setPartitionerClass(InvertedIndexPartitioner.class);
        job1.setReducerClass(InvertedIndexReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
        if(args.length < 3) {
            System.err.println("You can add an output address to run job2.");
            System.exit(0);
        }
        // JOB2：单词频率全局排序，输出前50个高频词
        Job job2 = Job.getInstance(conf, "Sort");
        job2.setJarByClass(InvertedIndexer.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setOutputKeyClass(DoubleWritable.class);
        job2.setOutputValueClass(Text.class);
        job2.setSortComparatorClass(SortDecreasingComparator.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.waitForCompletion(true);
        if(args.length < 4) {
            System.err.println("You can add an output address to run job3.");
            System.exit(0);
        }
        // JOB3：为每个作品计算每个单词的TF-IDF
        Job job3 = Job.getInstance(conf, "TF-IDF");
        job3.setJarByClass(InvertedIndexer.class);
        job3.setMapperClass(TFIDFMapper.class);
        job3.setReducerClass(TFIDFReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[1]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}