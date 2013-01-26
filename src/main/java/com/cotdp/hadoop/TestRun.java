package com.cotdp.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: girish.kathalagiri
 * Date: 1/23/13
 * Time: 7:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestRun {







  public static  void main(String args[])
  {

    Configuration conf = new Configuration();
    conf.set("mapred.job.reduce.memory.mb", "2048");
    // Standard stuff
    Job job = null;
    try {
      job = new Job(conf);
    } catch (IOException e) {
      e.printStackTrace();
    }
    assert job != null;
    job.setJobName("TestRun");
    job.setJarByClass(MyMapper.class);

    job.setMapperClass(MyMapper.class);

    //
    job.setInputFormatClass(ZipFileInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    //
    //ZipFileInputFormat.setInputPaths(job, new Path(inputPath, "zip-01.zip"));
    try {
      ZipFileInputFormat.setInputPaths(job, new Path(args[0]));
    } catch (IOException e) {
      e.printStackTrace();
    }
    try {
      TextOutputFormat.setOutputPath(job, new Path(args[1]));
    } catch (IOException e) {
      e.printStackTrace();
    }


    //
    try {
      job.waitForCompletion(true);
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }


}
