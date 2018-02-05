package com.relayhealth.mongoloader;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.relayhealth.hadoop.util.MongoMap;
import com.relayhealth.hadoop.util.MongoReduce;

public class MongoLoad {

  final static long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf();
    
    // The following line can influence the number of splits in the file.  The higher the divisor, the more split
    // files.  Currently we utilize the default as CosmosDB itself does not handle high volume rate at its
    // current configuration (thus would return an error).
    conf.setLong(FileInputFormat.SPLIT_MAXSIZE, conf.getLong(FileInputFormat.SPLIT_MAXSIZE, DEFAULT_SPLIT_SIZE) / 8);
    
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: <in> <out>");
      System.exit(2);
    }

    // Setup the Job instance, and provide a name so its progress can be tracked.
    Job job = Job.getInstance(conf, "MongoLoad");
    
    // Set the Mapper and Reducer classes
    job.setMapperClass(MongoMap.class);
    job.setReducerClass(MongoReduce.class);
    
    // Provide the main class for this jar
    job.setJarByClass(MongoLoad.class);

    // Set the input/output key/value pairs to Text
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Turn off the reducer tasks - we do not need it here
    job.setNumReduceTasks(0);

    // Define the input file-path
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    
    // Define the output file-path (so that temporary and success files can be written there)
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    
    // Launch the job and wait for completion.
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
