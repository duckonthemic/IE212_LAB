import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
 public class Assign3 {
 public static class CustsMapper extends Mapper <Object, Text, Text, Text>
 {
 	public void map(Object key, Text value, Context context)
 	throws IOException, InterruptedException 
 	{
 		String record = value.toString();
 		String[] parts = record.split(",");
 		context.write(new Text(parts[0]), new Text("cust   " + parts[1]));
 	}
 }
 
 public static class VIPMapper extends Mapper <Object, Text, Text, Text>
 {
 	public void map(Object key, Text value, Context context) 
 	throws IOException, InterruptedException 
 	{
 		String record = value.toString();
 		String[] parts = record.split(",");
 		context.write(new Text(parts[2]), new Text("tnxn   " + parts[4]));
 	}
 }
 
 public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text>
 {
 	public void reduce(Text key, Iterable<Text> values, Context context)
 	throws IOException, InterruptedException 
 	{
 		String name = "";
 		Map<String, Integer> gameTypeCount = new HashMap<>();
 		for (Text t : values) 
 	{ 
 	String parts[] = t.toString().split("   ");
 
 	if (parts[0].equals("tnxn")) 
 	{
	 	String gameType = parts[1];
	 	int currentCount;
	 	if (gameTypeCount.containsKey(gameType)) {
         	currentCount = gameTypeCount.get(gameType);
     	} 
	else 
	{
         currentCount = 0;
     	}
	 gameTypeCount.put(gameType, currentCount + 1);	 
 	} 
 	else if (parts[0].equals("cust")) 
 	{
 	 name = parts[1];
 	}
 }
 	if (!name.isEmpty()) {
     	for (Map.Entry<String, Integer> entry : gameTypeCount.entrySet()) 
	{
         	String gameType = entry.getKey();
         	int count = entry.getValue();
         	// Output: (game_type, customer_name \t count)
         	context.write(new Text(gameType), new Text(name + "\t" + count));
     	}
 }
 }
 }
 
 public static class VipFilterMapper extends Mapper<Object, Text, Text, Text> 
 {
 	private static final int VIP_THRESHOLD = 2; // threshold to VIP
 	protected void map(Object key, Text value, Context context)
         
	throws IOException, InterruptedException 
	{
         	String record = value.toString().trim();
         	String[] parts = record.split("\t");
         	if (parts.length != 3) 
		{
             		return; 
        	}
         	String gameType = parts[0].trim();
         	String customerName = parts[1].trim();
         	int count = 0;
         	try 
		{
             		count = Integer.parseInt(parts[2].trim());
         	} 
		catch (NumberFormatException e) 
		{
             		return;
         	}
         if (count >= VIP_THRESHOLD) 
	 {
             context.write(new Text(gameType), new Text(customerName));
         }
     }
 }
 
 public static class AggregateVipReducer extends Reducer<Text, Text, Text, Text> 
 {
     protected void reduce(Text key, Iterable<Text> values, Context context)
             throws IOException, InterruptedException {

         List<String> vipCustomers = new ArrayList<>();
         for (Text t : values) {
             vipCustomers.add(t.toString());
         }
         StringBuilder sb = new StringBuilder();
         for (int i = 0; i < vipCustomers.size(); i++) {
             sb.append(vipCustomers.get(i));
             if (i < vipCustomers.size() - 1) {
                 sb.append(", ");
             }
         }
         context.write(key, new Text(sb.toString()));
     }
  }
 
 public static void main(String[] args) throws Exception 
 {
 Configuration conf = new Configuration();
 
 Job job1 = Job.getInstance(conf, "Reduce-side join");
 
 job1.setJarByClass(VIP_cust.class);
 job1.setReducerClass(ReduceJoinReducer.class);
 job1.setOutputKeyClass(Text.class);
 job1.setOutputValueClass(Text.class);
  
 
 MultipleInputs.addInputPath(job1, new Path(args[0]),TextInputFormat.class, CustsMapper.class);
 MultipleInputs.addInputPath(job1, new Path(args[1]),TextInputFormat.class, VIPMapper.class);
 
 String intermediateOutput = "intermediate_output_" + System.currentTimeMillis();
 FileOutputFormat.setOutputPath(job1, new Path(intermediateOutput));
 
 boolean success = job1.waitForCompletion(true);
 if (!success) {
     System.err.println("Job1 failed, exiting");
     System.exit(1);
 }
 
 Job job2 = Job.getInstance(conf, "Aggregate VIP Customers");
 job2.setJarByClass(Assign3.class);

 job2.setMapperClass(VipFilterMapper.class);
 job2.setReducerClass(AggregateVipReducer.class);
 job2.setOutputKeyClass(Text.class);
 job2.setOutputValueClass(Text.class);

 
 FileInputFormat.addInputPath(job2, new Path(intermediateOutput));
 FileOutputFormat.setOutputPath(job2, new Path(args[2]));
 
 job2.setNumReduceTasks(1); 

 
 System.exit(job2.waitForCompletion(true) ? 0 : 1);
 }
 }