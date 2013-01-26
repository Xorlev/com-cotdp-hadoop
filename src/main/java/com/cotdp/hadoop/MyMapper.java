package com.cotdp.hadoop;

/**
 * Created with IntelliJ IDEA.
 * User: girish.kathalagiri
 * Date: 1/23/13
 * Time: 8:42 AM
 * To change this template use File | Settings | File Templates.
 */

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * This Mapper class checks the filename ends with the .txt extension, cleans
 * the text and then applies the simple WordCount algorithm.
 *
 */
public  class MyMapper
  extends Mapper<Text, BytesWritable, NullWritable, Text>
{

  @Override
  public void map( Text key, BytesWritable value, Context context )
    throws IOException, InterruptedException
  {


    byte[] data = value.getBytes();
    context.write(null, new Text(data));


  }
}