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


/*  思路解释：Map1-Reduce1将有向边统计为无向边。 输出格式："a b" 表示a和b之间有无向边
*   Map2-Reduce2查找需要查询的边。考虑点a有b,c两个邻居(a<b<c)的情况，则输出中含有"b+c -"表示需要查找是否有bc这条边；同时会将原有的图中边输出，格式为"a+b +","a+c +"等。
*   Map3-Reduce3进行最后的计算。Map3不做操作; 在Reduce3中，一个键a+b，如果存在值为+的元素，则表示存在这条边；如果存在值为-的元素，则表示存在一个三角形由这条边构成。统计-的个数，如果+存在，则总结果加上-的数量。
*/
public class TriCount_old {
    public static String input_address;

    /*读取 边1-*/
    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        //边统计实现
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            String a = itr.nextToken();
            String b = itr.nextToken();
            //最终存储时，a一定是小编号，b一定是大编号。
            //flag是节点序标记，取值=1表示大指向小，=0表示小指向大
            String flag = "0";
            if (!a.equals(b)) {
                if (a.compareTo(b) > 0) {
                    flag = "1";
                    String tmp = a;
                    a = b;
                    b = tmp;
                }
                context.write(new Text(a + '\t' + b), new Text(flag));
            }
        }
    }

    //Reduce作用：将K2,V2 转为 K3,V3
    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String[] term = key.toString().split("\t");

            boolean flag0 = false, flag1 = false;
            for (Text val : values) {
                if (val.toString().equals("0")) flag0 = true;
                else flag1 = true;
            }
            Text a = new Text(term[0]);
            Text b = new Text(term[1]);
            if (flag0 && flag1) {
                context.write(a, b);
            }
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, Text, Text>{
        private Text newKey = new Text();
        private Text newValue = new Text();
        @Override
        public void map(LongWritable key,Text value,Context context)//没做什么，只是为了执行reduce
                throws IOException,InterruptedException
        {
            StringTokenizer itr=new StringTokenizer(value.toString());
            newKey.set(itr.nextToken());
            newValue.set(itr.nextToken());
            context.write(newKey, newValue);
        }

    }

    public static class Reduce2 extends Reducer<Text,Text,Text,Text>{
        //value为"+"表示原图中有这条边；为"-"表示需要寻找这条边。
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            ArrayList<String> dest = new ArrayList<>();
            Text curKey = new Text();
            Text curValue = new Text();
            curValue.set("+");
            for(Text value:values){
                dest.add(value.toString());
                curKey.set(key.toString()+"+"+value.toString());
                context.write(curKey,curValue);
            }
            curValue.set("-");
            int size = dest.size();
            for(int i=0;i<size;++i){
                for(int j=i+1;j<size;++j){
                    curKey.set(dest.get(i)+"+"+dest.get(j));
                    context.write(curKey,curValue);
                }
            }
        }
    }

    public static class Map3 extends Mapper<LongWritable, Text, Text, Text>{
        private Text newKey = new Text();
        private Text newValue = new Text();
        @Override
        public void map(LongWritable key,Text value,Context context)//没做什么，只是为了执行reduce
                throws IOException,InterruptedException
        {
            StringTokenizer itr=new StringTokenizer(value.toString());
            newKey.set(itr.nextToken());
            newValue.set(itr.nextToken());
            context.write(newKey, newValue);
        }
    }
    public static class Reduce3 extends Reducer<Text, Text,Text,Text>{
        private int result = 0;
        @Override
        public void cleanup(Context context)throws IOException, InterruptedException{
            context.write(new Text("result: "),new Text(""+result));
        }
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            int cnt = 0;
            boolean flag = false;
            for(Text value:values){
                if(value.toString().equalsIgnoreCase("+")){
                    flag = true;
                }else{
                    cnt++;
                }
            }
            if(flag) result+=cnt;
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        input_address = args[0];

        Job job1 = Job.getInstance(conf, "job1");
        job1.setJarByClass(TriCount_old.class);
        job1.setMapperClass(Map1.class);
        job1.setReducerClass(Reduce1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(input_address));
        FileOutputFormat.setOutputPath(job1, new Path("./output/out1"));

        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf,"job2");
        job2.setJarByClass(TriCount_old.class);
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("./output/out1"));
        FileOutputFormat.setOutputPath(job2, new Path("./output/out2"));

        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(conf,"job3");
        job3.setJarByClass(TriCount_old.class);
        job3.setMapperClass(Map3.class);
        job3.setReducerClass(Reduce3.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3,new Path("./output/out2"));
        FileOutputFormat.setOutputPath(job3, new Path("./output/finalOut"));
        job3.waitForCompletion(true);

    }

}
