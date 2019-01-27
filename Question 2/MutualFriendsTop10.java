import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;
public class MutualFriendsTop10{

	public static class Mapper1 
			extends Mapper<LongWritable, Text, Text, Text>{
		private Text user = new Text();
		private Text friends = new Text();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException{
			/*Splitting each line into user and corresponding friend list*/
			String[] split=value.toString().split("\\t");
			/*split[0] is user and split[1] is friend list */
			String userId=split[0];
			if(split.length==1) {
				return;
			}
			String[] friendIds=split[1].split(",");
			for(String friend : friendIds) {
				if(userId.equals(friend)) {
					continue;
				}
			String userKey = (Integer.parseInt(userId) < Integer.parseInt(friend))?userId + "," +friend : friend + ","+ userId;
			friends.set(split[1]);
			user.set(userKey);
			context.write(user,friends);
			}
		}
	}
		
	public static class Reducer1
			extends Reducer<Text, Text, Text, Text>{
		
		private int matchingFriends(String firstList, String secondList) {
			
			if(firstList == null || secondList == null) {
				return 0;
			}
			
			String[] list1=firstList.split(",");
			String[] list2=secondList.split(",");
			
			LinkedHashSet<String> firstSet = new LinkedHashSet();
			for(String  user: list1) {
				firstSet.add(user);
			}
			/*Retaining sort order*/
			LinkedHashSet<String> secondSet = new LinkedHashSet();
			for(String  user: list2) {
				secondSet.add(user);
			}
			firstSet.retainAll(secondSet);
			/*Keeping only the matched friends*/
			return firstSet.size();
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException,InterruptedException{
			
			String[] friendsList = new String[2];
			int index=0;
			
			for(Text value:values) {
				friendsList[index++] = value.toString();
			}
			int  mutualFriends = matchingFriends(friendsList[0],friendsList[1]);
			context.write(key, new Text(Integer.toString(mutualFriends)));
						
		}
		
	}
	
	public static class Mapper2
		extends Mapper<LongWritable, Text, IntWritable, Text>{
		
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException{
			
			context.write(one, value);
		}
		
	}
	//output the top 10 mutual friends
	public static class Reducer2 
		extends Reducer<IntWritable, Text, Text, IntWritable>{
		
		public void reduce(IntWritable key, Iterable<Text> values, 
				Reducer<IntWritable, Text, Text, IntWritable>.Context context) 
		throws IOException, InterruptedException{
			
		HashMap<String, Integer> map = new HashMap();
		for(Text value:values) {
			String[] lines = value.toString().split("\t");
			if(lines.length == 2) {
				map.put(lines[0],Integer.parseInt(lines[1]));
			}
		}
		ValueComparator comparator = new ValueComparator(map);
		TreeMap<String, Integer> mapSortedByValues = new TreeMap<String, Integer>(comparator);
		mapSortedByValues.putAll(map);
		
		int count = 1;
		for(Map.Entry<String, Integer> entry: mapSortedByValues.entrySet()) {
			if(count<=10) {
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}
			else
					break;
				count++;
			}
		}
		
	   }
	
	
		public static class ValueComparator implements Comparator<String>{
			HashMap<String, Integer> base;
			public ValueComparator(HashMap<String, Integer> base) {
				this.base = base;
			}
			
			public int compare(String first, String second) {
				if(base.get(first) >= base.get(second)){
					return -1;
				}
								
				else {
					return 1;
				}
				
			}
		}
		


	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: MutualFriendsTop10 <in> <out> <temp>");
			System.exit(2);
		}
		
		String inputPath = otherArgs[0];
		String outputPath = otherArgs[1];
		String tempPath = otherArgs[2];
		 //First Job
        {	//create first job
			conf = new Configuration();
			Job job = new Job(conf, "MutualFriends");
			job.setJarByClass(MutualFriendsTop10.class);
			job.setMapperClass(Mapper1.class);
			job.setReducerClass(Reducer1.class);
			
			 //set job1's mapper output key type
			job.setMapOutputKeyClass(Text.class);
			 //set job1's mapper output value type
			job.setMapOutputValueClass(Text.class);
			
			// set job1's output key type
			job.setOutputKeyClass(Text.class);
			// set job1's output value type
			job.setOutputValueClass(Text.class);
			//set job1's input HDFS path
			FileInputFormat.addInputPath(job, new Path(inputPath));
			// set the HDFS path for the job1's output
			FileOutputFormat.setOutputPath(job, new Path(tempPath));
			
			if(!job.waitForCompletion(true))
                System.exit(1);
			
		}
        
        //Second Job
        {
            conf = new Configuration();
            Job job2 = new Job(conf, "TopTenMutualFriends");

            job2.setJarByClass(MutualFriendsTop10.class);
    		job2.setMapperClass(Mapper2.class);
    		job2.setReducerClass(Reducer2.class);
    		  		
    		
    		 //set job2's mapper output key type
    		job2.setMapOutputKeyClass(IntWritable.class);
    		 //set job2's mapper output value type
    		job2.setMapOutputValueClass(Text.class);
    		//set job2's output key type
    		job2.setOutputKeyClass(Text.class);
    		//set job2's output value type
    		job2.setOutputValueClass(IntWritable.class);
    		
    		job2.setInputFormatClass(TextInputFormat.class);
    		
    		  //job2's input is job1's output
    		FileInputFormat.addInputPath(job2, new Path(tempPath));
    		 //set job2's output path
    		FileOutputFormat.setOutputPath(job2, new Path(outputPath));	
    		System.exit(job2.waitForCompletion(true) ? 0 : 1);   
                      
        }
		
	}
}