package joinMapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class reducejoinDriver {
	
	 public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>
	 {

		
		 private Text word1 = new Text();
		 private Text word2 = new Text();
         String keyTemp,valuetemp;    
		 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			 FileSplit fileSplit = (FileSplit)context.getInputSplit();
			 String filename = fileSplit.getPath().getName();
			 
			 String[] words=value.toString().split(" ");
			 keyTemp=words[0]+"_"+filename;
			 valuetemp=words[1];
			 word1.set(keyTemp);
			 word2.set(valuetemp); 	 
				 context.write(word1, word2);
			 
			 
		 }
	 }
	 public class TaggedJoiningPartitioner extends Partitioner<Text,Text> {

		    @Override
		    public int getPartition(Text taggedKey, Text text, int numPartitions) {
		       String[] taggedkey=taggedKey.toString().split("_");
			   
		       if(taggedkey[0]=="x")
		    	   return 0;
		       else
		    	   return 1;
		       
		    }
		}

	 public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
		  
			private IntWritable result = new IntWritable();
			private Map<String,ArrayList<String>> mapp=new HashMap<String,ArrayList<String>>();
			private ArrayList<String> templist=new ArrayList<String>();
			
			
			public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
					int sum = 0;
					for (Text val : values) {
							templist.add(val.toString());
					}
					mapp.put(key.toString().split("_")[0], templist);
					
			}
			
			@Override
		    protected void cleanup(Context context) throws IOException, InterruptedException {
				 Text keyText = new Text();
				Set<String> tempkey=mapp.keySet();
				for(String key:tempkey){
					keyText.set(mapp.get(key));
					context.write(key, keyText);
				}
		    }
			
		}	
	
	
	
	
	
	
	
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(reducejoinDriver.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	   // FileInputFormat.addInputPath(job, new Path(args[0]));
	    
	    FileSystem fs= FileSystem.get(conf); 
	    FileStatus[] status_list = fs.listStatus(new Path("hdfs://push-sqld:9000/inverted"));
	    if(status_list != null){
	        for(FileStatus status : status_list){
	            FileInputFormat.addInputPath(job, status.getPath());
	        }
	    }
	    
	    FileOutputFormat.setOutputPath(job, new Path("hdfs://push-sqld:9000/inverted/output"));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
