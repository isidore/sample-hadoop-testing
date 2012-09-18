package com.gistlabs.bigdata.littletests.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WordCountMapper extends SmartMapper<LongWritable, Text, Text, LongWritable>
{
  @Override
  protected void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException
  {
    for (String token : text.toString().split("\\s+"))
    {
      context.write(new Text(token), new LongWritable(1));
    }
  }
  @Override
  public Class<LongWritable> getKeyInType()
  {
    return LongWritable.class;
  }
  @Override
  public Class<Text> getValueInType()
  {
    return Text.class;
  }
  @Override
  public Class<Text> getKeyOutType()
  {
    return Text.class;
  }
  @Override
  public Class<LongWritable> getValueOutType()
  {
    return LongWritable.class;
  }
}