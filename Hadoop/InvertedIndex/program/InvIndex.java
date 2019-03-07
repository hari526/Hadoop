package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.*;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class InvIndex extends Configured implements Tool {

public static void main(String[] args) throws Exception {
int res = ToolRunner.run(new InvIndex(), args);
System.exit(res);
}

public int run(String[] args) throws Exception {

Job job = Job.getInstance(getConf(), "InvIndex");
job.setJarByClass(this.getClass());
// Use TextInputFormat, the default unless job.setInputFormatClass is used
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

job.setMapperClass(Map.class);
job.setReducerClass(Reduce.class);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(Text.class);

return job.waitForCompletion(true) ? 0 : 1;
}


public static class Map extends Mapper<LongWritable, Text, Text, Text> {

@Override

public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException
 {
 	String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
	String line = value.toString();
	String words[] = line.split(" ");

	for (String word : words) 
	{
		context.write(new Text(word),new Text(filename));
	}
 }	
}

public static class Reduce extends Reducer<Text, Text, Text, Text> {
@Override
public void reduce(Text key, Iterable<Text> values, Context context)
throws IOException, InterruptedException 
{
int count = 0;
HashMap hp = new HashMap();
for (Text c : values) 
	{
	String stri = c.toString();
	if(hp!=null && hp.get(stri)!=null)
	{
		count =(int)hp.get(stri);
		hp.put(stri,++count);
	}
	else
	{
		hp.put(stri,1); 
	}
	
	}
      context.write(key, new Text(hp.toString()));
}
}

}
















