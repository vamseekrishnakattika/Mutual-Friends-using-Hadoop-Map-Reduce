import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class DirectFriendsMinAgeTop10{
	
	public static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {
		Text user = new Text();
		Text friends = new Text();
		
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
				user.set(userId);
				friends.set(friend);
				context.write(user,friends);
				}		
			
			}		
	} 


	public static class Reducer1 extends Reducer<Text, Text, Text, Text> {
		
		HashMap<String,String> age = new HashMap<String,String>(); //store userID,age in this HashMap in memory
		
		public static int getAgeFromDOB(Date first, Date last) {
		   
		    int difference =(int)daysBetween(first,last);
		    return difference;
		}

		private static long daysBetween(Date one, Date two) { 
			
			long difference = (one.getTime()-two.getTime())/86400000; 
			return Math.abs(difference); 			
		}

		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			//read data to memory on the reducer1.
			Configuration conf = context.getConfiguration();
			
			Path part=new Path(context.getConfiguration().get("ARGUMENT"));//Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split(",");
		        			        	
		        	age.put(arr[0], arr[9]); //put userID,date of birth in HashMap variable
		        	line=br.readLine();
		        }
		    }
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			TreeMap<Integer,String> findMinAge = new TreeMap<Integer,String>(); //This TreeMap for finding Minimum age
			for (Text friend : values) {
				if (age.containsKey(friend.toString())) {
					
					Date today = new Date();
					String DOB = age.get(friend.toString());
					Date date1 = null;
					try {
						date1 = new SimpleDateFormat("MM/dd/yyyy").parse(DOB);
					} 
					catch (ParseException e) {
						
						e.printStackTrace();
					}  
					
					int userAge = getAgeFromDOB(date1,today);
					
					findMinAge.put(userAge, friend.toString()); //TreeMap automatically sorts by key: ie userAge
				}
					
			}
			int minAge = findMinAge.firstKey();
			String minAgeUser= findMinAge.get(minAge);
			String val = minAgeUser+":"+minAge;
			context.write(key, new Text(val));
		}
		
		
		
		
	}
	//job2's mapper swap key and value, sort by key - automatically done in ascending order.
	public static class Mapper2 extends Mapper<Text, Text, LongWritable, Text> {

		  private LongWritable freq = new LongWritable();

		  public void map(Text key, Text value, Context context)
		    throws IOException, InterruptedException {
			String[] val = value.toString().split(":");
			int newVal = Integer.parseInt(val[1]);		  
		    freq.set(newVal);
		    String newKey = key.toString()+":"+val[0];
		    context.write(freq, new Text(newKey));
		  }
		}
	
	//output the top 10 users according to minimum age
	//Also join with userID to get Addresses
	public static class Reducer2 extends Reducer<LongWritable, Text, Text, Text> {
		
		private int index = 0;		
		HashMap<String,String> address = new HashMap<String,String>();
		Text user = new Text();
		Text details = new Text();
		
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
			//read data to memory on the reducer2.
			Configuration conf = context.getConfiguration();
			
			Path part=new Path(context.getConfiguration().get("ARGUMENT"));//Location of file in HDFS
			
			
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] fss = fs.listStatus(part);
		    for (FileStatus status : fss) {
		        Path pt = status.getPath();
		        
		        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
		        line=br.readLine();
		        while (line != null){
		        	String[] arr=line.split(",");
		        	//put (userID, address in the HashMap variable
		        	
		        	address.put(arr[0], arr[3]+", "+arr[4]+", "+arr[5]);
		        	line=br.readLine();
		        }
		    }
		}
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context)
		      throws IOException, InterruptedException {
			
			for (Text value : values) {
		    	if (index < 10) {
		    		index++;
		    		String[] str = value.toString().split(":");
		    		String addDetails = null;
		    		String userId = null;
		    		if(address.containsKey(str[0])) {
		    			userId= str[0];
		    			int age = Integer.parseInt(key.toString());
		    			int finalAge = age/365;
		    			addDetails = address.get(str[0])+", Friend ID: "+str[1]+" Min Age: "+Integer.toString(finalAge);
		    		}
		    		user.set(userId);
		    		details.set(addDetails);
		    		context.write(user, details);
		    	}
		    }
		  }
			
	}
	
	
    public static void main(String []args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
			System.err.println("Usage: DirectFriendsMinAgeTop10 <in> <userdata> <out> <temp>");
			System.exit(2);
		}
        
        conf.set("ARGUMENT",otherArgs[1]);
                
        //First Job
        {	//create first job
            conf = new Configuration();
            conf.set("ARGUMENT",otherArgs[1]);
            @SuppressWarnings("deprecation")
            Job job = new Job(conf, "Job1");

            job.setJarByClass(DirectFriendsMinAgeTop10.class);
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);
            
            //set job1's mapper output key type
            job.setMapOutputKeyClass(Text.class);
            //set job1's mapper output value type
            job.setMapOutputValueClass(Text.class);
            
            // set job1;s output key type
            job.setOutputKeyClass(Text.class);
            // set job1's output value type
            job.setOutputValueClass(Text.class);
            //set job1's input HDFS path
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            //job1's output path
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

            if(!job.waitForCompletion(true))
                System.exit(1);
        }
        //Second Job
        {
            conf = new Configuration();
            conf.set("ARGUMENT",otherArgs[1]);
            @SuppressWarnings("deprecation")
            Job job2 = new Job(conf, "Job2");

            job2.setJarByClass(DirectFriendsMinAgeTop10.class);
            job2.setMapperClass(Mapper2.class);
            job2.setReducerClass(Reducer2.class);
            
            //set job2's mapper output key type
            job2.setMapOutputKeyClass(LongWritable.class);
            //set job2's mapper output value type
            job2.setMapOutputValueClass(Text.class);
            
            //set job2's output key type
            job2.setOutputKeyClass(Text.class);
            //set job2's output value type
            job2.setOutputValueClass(Text.class);

            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            
            
            job2.setNumReduceTasks(1);
            //job2's input is job1's output
            FileInputFormat.addInputPath(job2, new Path(otherArgs[3]));
            //set job2's output path
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}