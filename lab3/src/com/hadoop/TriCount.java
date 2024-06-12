package com.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;


/* 选做内容。 */
public class TriCount {
    public static String input_address;

    /*读取 边1-*/
    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        //边统计实现
        private Text newKey = new Text();
        private Text newValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String a = itr.nextToken();
            String b = itr.nextToken();
            if (a.compareTo(b) == 0) return;//去除起点终点相同的边
            if (a.compareTo(b) > 0) //交换ab，保证a<b
            {
                String temp = a;
                a = b;
                b = temp;
            }
            newKey.set(a + "+" + b);
            newValue.set("+");
            context.write(newKey, newValue);
        }

    }

    //Reduce作用：将K2,V2 转为 K3,V3
    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
        //选做内容。因为在map中记录时都是小序号在前，所以一个key就代表一条边
        private Text newValue = new Text("+");

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, newValue);
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] tokens = itr.nextToken().split("\\+"); //以+为分隔符分割a和b
            Text newKey = new Text();
            Text newValue = new Text();
            newKey.set(tokens[0]);
            newValue.set(tokens[1]);
            context.write(newKey, newValue);
        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
        //value为"+"表示原图中有这条边；为"-"表示需要寻找这条边。
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            ArrayList<String> array = new ArrayList<String>();
            Text newKey = new Text();
            Text newValue = new Text();
            newValue.set("+");
            for (Text value : values) {
                array.add(value.toString());//有相同定点的边加入array集合
                newKey.set(key.toString() + "+" + value.toString());//处理过的变为map执行之前的形式
                context.write(newKey, newValue);
            }
            for (int i = 0; i < array.size(); i++) //对于任意两点构造查询键值对
            {
                for (int j = i + 1; j < array.size(); j++) {
                    String a = array.get(i);
                    String b = array.get(j);
                    if (a.compareTo(b) < 0)//保证ab的大小关系，减号表示需要查询
                    {
                        newKey.set(a + "+" + b);
                        newValue.set("-");
                    } else {
                        newKey.set(b + "+" + a);
                        newValue.set("-");
                    }
                    context.write(newKey, newValue);
                }
            }
        }
    }

    public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
        private Text newKey = new Text();
        private Text newValue = new Text();

        public void map(LongWritable key, Text value, Context context)//没做什么，只是为了执行reduce
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            newKey.set(itr.nextToken().toString());
            newValue.set(itr.nextToken().toString());
            context.write(newKey, newValue);
        }
    }

    public static class Reduce3 extends Reducer<Text, Text, Text, Text> {
        private static int result = 0;

        public void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Result: "), new Text("" + result)); //写入结果
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int cnt = 0;
            boolean flag = false;
            for (Text value : values) {
                if (value.toString().equalsIgnoreCase("+")) //存在这条边
                    flag = true;
                else if (value.toString().equalsIgnoreCase("-")) //需要查询这条边
                    cnt++;
            }
            if (flag) result += cnt;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        input_address = args[0];

        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(TriCount.class);
        job1.setMapperClass(Map1.class);
        job1.setReducerClass(Reduce1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(input_address));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"out1"));

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "job2");
        job2.setJarByClass(TriCount_full.class);
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]+"out1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"out2"));

        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(conf, "job3");
        job3.setJarByClass(TriCount_full.class);
        job3.setMapperClass(Map3.class);
        job3.setReducerClass(Reduce3.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(args[1]+"out2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]+"finalOut"));
        job3.waitForCompletion(true);

    }

}
