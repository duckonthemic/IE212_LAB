import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
 public class maxGameType 
 {
 
	//read trans file and show the list of: Game_type Total_amount
 public static class TransMapper extends Mapper <Object, Text, Text, Text>
 {
 	public void map(Object key, Text value, Context context) 
 	throws IOException, InterruptedException 
 	{
 		String record = value.toString().trim();
 		String[] parts = record.split(",");
 
 		String gametype = parts[4];
 		String amount = parts[3];
 
 		context.write(new Text(gametype), new Text(amount));
 	}
 }
 
 public static class TransReducer extends Reducer <Text, Text, Text, Text>
 {
	 int max = 0;
	 Text maxGame = new Text();
	 double maxtotal = 0.0;

 public void reduce(Text key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException 
 {

	 double total = 0.0;
	 int count = 0;
	 for (Text t : values) 
	 { 
		 count++;
		 total += Float.parseFloat(t.toString().trim());
		 
		 if (max < count) 
		{
			 max = count;
			 maxGame.set(key);
			 maxtotal = total;
		 }	 
	 }
 }
 @Override
 protected void cleanup(Context context) throws IOException, InterruptedException 
 {
	 String str = String.format("%d %f", max, maxtotal);
     context.write(maxGame, new Text(str));
 }
 }
 
 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job =  Job.getInstance(conf, "Max player game type");
 job.setJarByClass(Assign4.class);
 job.setMapperClass(TransMapper.class);
 job.setReducerClass(TransReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);
 
 //job.setNumReduceTasks(0);
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
 }