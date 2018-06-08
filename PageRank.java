package com.gokul.hdp.pagerank;
import java.io.*;
import java.text.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
public class PageRank extends Configured implements Tool
{
        public PageRank(){}
        public static NumberFormat NF = new DecimalFormat("00");
        public static Set<String> NODES = new HashSet();
        public static String LINKS_SEPARATOR = "|";
        public static Double DAMPING = Double.valueOf(0.85D);
        public static int ITERATIONS = 2;
public static class First_Mapper extends Mapper<LongWritable, Text, Text, Text> {
	    @Override
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        if (value.charAt(0) != '#') {
	            int tabIndex = value.find("\t");
	            String first_node = Text.decode(value.getBytes(), 0, tabIndex);
	            String second_node = Text.decode(value.getBytes(), tabIndex + 1, value.getLength() - (tabIndex + 1));
	            context.write(new Text(first_node), new Text(second_node));
	            PageRank.NODES.add(first_node);
	            PageRank.NODES.add(second_node);
	          }
	    }
}
public static class First_Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         boolean first = true;
         String links = (PageRank.DAMPING / PageRank.NODES.size()) + "\t";
         for (Text value : values) {
            if (!first) 
                links += ",";
            links += value.toString();
            first = false;
        }
    context.write(key, new Text(links));
    }
}
public static class Second_Mapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int tIdx1 = value.find("\t");
        int tIdx2 = value.find("\t", tIdx1 + 1);
        String page = Text.decode(value.getBytes(), 0, tIdx1);
        String pageRank = Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1));
        String links = Text.decode(value.getBytes(), tIdx2 + 1, value.getLength() - (tIdx2 + 1));
        String[] allOtherPages = links.split(",");
        for (String otherPage : allOtherPages) { 
            Text pageRankWithTotalLinks = new Text(pageRank + "\t" + allOtherPages.length);
            context.write(new Text(otherPage), pageRankWithTotalLinks); 
        }
        context.write(new Text(page), new Text(PageRank.LINKS_SEPARATOR + links));
    }
}
public static class Second_Reducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
                                                                                InterruptedException {
    String links = "";
    double sumShareOtherPageRanks = 0.0;
    for (Text value : values) {
        String content = value.toString();
            if (content.startsWith(PageRank.LINKS_SEPARATOR)) {
                links += content.substring(PageRank.LINKS_SEPARATOR.length());
            } else {
                    String[] split = content.split("\\t");
                    double pageRank = Double.parseDouble(split[0]);
                    int totalLinks = Integer.parseInt(split[1]);
                    sumShareOtherPageRanks += (pageRank / totalLinks);
            }
        }
        double newRank = PageRank.DAMPING * sumShareOtherPageRanks + (1 - PageRank.DAMPING);
        context.write(key, new Text(newRank + "\t" + links));
    }
}
public static class Third_Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int tIdx1 = value.find("\t");
        int tIdx2 = value.find("\t", tIdx1 + 1);
        String page = Text.decode(value.getBytes(), 0, tIdx1);
        float pageRank = Float.parseFloat(Text.decode(value.getBytes(), tIdx1 + 1, tIdx2 - (tIdx1 + 1)));
        context.write(new DoubleWritable(pageRank), new Text(page));
    }
}
  public static void main(String[] args)
    throws Exception
  {
   	ToolRunner.run(new Configuration(), new PageRank(), args);
  }
  
@Override
public int run(String[] args) throws Exception {
	  Job first_job = Job.getInstance(new Configuration(), "FirstJob");
	  first_job.setJarByClass(PageRank.class);
	  FileInputFormat.setInputPaths(first_job,new Path(args[0]));
   	  FileOutputFormat.setOutputPath(first_job,new Path(args[1]+"/f0"));
      first_job.setInputFormatClass(TextInputFormat.class);
	  first_job.setMapOutputKeyClass(Text.class);
	  first_job.setMapOutputValueClass(Text.class);
	  first_job.setMapperClass(First_Mapper.class);
	  first_job.setOutputFormatClass(TextOutputFormat.class);
	  first_job.setOutputKeyClass(Text.class);
	  first_job.setOutputValueClass(Text.class);
	  first_job.setReducerClass(First_Reducer.class);
      first_job.waitForCompletion(true);
      for(int i=0;i<2;i++)
        {
	       Job second_job = Job.getInstance(new Configuration(), "SecondJob");
	       second_job.setJarByClass(PageRank.class);
	       FileInputFormat.setInputPaths(second_job,new Path(args[1]+"/f"+i));
	       FileOutputFormat.setOutputPath(second_job,new Path(args[1]+"/f"+(i+1)));
	       second_job.setInputFormatClass(TextInputFormat.class);
	       second_job.setMapOutputKeyClass(Text.class);
	       second_job.setMapOutputValueClass(Text.class);
	       second_job.setMapperClass(Second_Mapper.class);
	       second_job.setOutputFormatClass(TextOutputFormat.class);
	       second_job.setOutputKeyClass(Text.class);
	       second_job.setOutputValueClass(Text.class);
	       second_job.setReducerClass(Second_Reducer.class);
	       second_job.waitForCompletion(true);
        }
	            Job third_job = Job.getInstance(new Configuration(), "ThirdJob");
	            third_job.setJarByClass(PageRank.class);
	            third_job.setInputFormatClass(TextInputFormat.class);
	            third_job.setMapOutputKeyClass(DoubleWritable.class);
	            third_job.setMapOutputValueClass(Text.class);
                third_job.setMapperClass(Third_Mapper.class);
                FileInputFormat.setInputPaths(third_job,new Path(args[1]+"/f2"));
                FileOutputFormat.setOutputPath(third_job,new Path(args[1]+"/final_result"));
                third_job.setOutputFormatClass(TextOutputFormat.class);
	            third_job.setOutputKeyClass(DoubleWritable.class);
	            third_job.setOutputValueClass(Text.class);
	            third_job.waitForCompletion(true);
	            return 0;
}
}