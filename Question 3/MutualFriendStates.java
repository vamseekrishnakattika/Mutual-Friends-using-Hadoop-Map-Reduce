import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;
public class MutualFriendStates{

	public static class Map 
			extends Mapper<LongWritable, Text, Text, Text>{
		Text user = new Text();
		Text friends = new Text();
		HashMap<String,String> map = new HashMap<String,String>();
		
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
			String string = new String();
			
			for(String friend : friendIds) {
					string = string + map.get(friend)+",";
				}
			string= string.substring(0, string.length() - 1);	
			
			for(String friend : friendIds) {
				if(userId.equals(friend)) {
					continue;
				}
							
			String userKey = (Integer.parseInt(userId) < Integer.parseInt(friend))?userId + "," +friend : friend + ","+ userId;
								
			user.set(userKey);
			friend = map.get(friend);
			String regex="((\\b"+ friend + "[^\\w]+)|\\b,?" + friend + "$)";
			friends.set(string.replaceAll(regex, ""));
			context.write(user,friends);
			}
		}
		
		@Override
		protected void setup(Context context)
			throws IOException, InterruptedException{
				super.setup(context);
				Configuration conf = context.getConfiguration();
				Path part = new Path(context.getConfiguration().get("ARGUMENT"));
				
				FileSystem fs = FileSystem.get(conf);
				FileStatus[] fss = fs.listStatus(part);
				for(FileStatus status : fss) {
					Path pt = status.getPath();
					BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
					String line;
					line = br.readLine();
					while(line != null) {
						String[] arr = line.split(",");
						map.put(arr[0], arr[0]+":"+arr[1]+": "+arr[5]);
						line = br.readLine();
					}
				}
		}
	}
	
	
		
	public static class Reduce
			extends Reducer<Text, Text, Text, Text>{
		
		private String matchingFriends(String firstList, String secondList) {
			
			if(firstList == null || secondList == null) {
				return null;
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
			return firstSet.toString().replaceAll("\\[|\\]", "");
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException,InterruptedException{
			
			String[] friendsList = new String[2];
			int index=0;
			
			for(Text value:values) {
				friendsList[index++] = value.toString();
			}
			String mutualFriends = matchingFriends(friendsList[0],friendsList[1]);	
			String[] finalFriendList = mutualFriends.split(",");
			String finalFriends = new String();
			for(String str : finalFriendList) {
				String subStr = str.substring(str.indexOf(':')+1);
				finalFriends = finalFriends + subStr +", ";
			}
			finalFriends= finalFriends.substring(0, finalFriends.length() - 2);
			if(mutualFriends != null && mutualFriends.length() != 0) {
				context.write(key, new Text("["+finalFriends+"]"));
			}
			
			
		}
		
	}

	


	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 3) {
			System.err.println("Usage: MutualFriendStates <input file name> <userdata file> <output file name>");
			System.exit(2);
		}
		conf.set("ARGUMENT", otherArgs[1]);
		// create a job with name "mutual friends states"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "mutualfriends states");
		job.setJarByClass(MutualFriendStates.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);


		// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);


		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(Text.class);
		//set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		//Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}