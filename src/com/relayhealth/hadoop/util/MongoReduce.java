package com.relayhealth.hadoop.util;


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * A NoOp Class which extends the Reducer object but does no actual work
 * @author lawrence.spiwak@relayhealth.com
 *
 */
public class MongoReduce extends Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
  }
}
