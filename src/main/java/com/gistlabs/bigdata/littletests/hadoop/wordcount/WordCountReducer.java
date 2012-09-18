package com.gistlabs.bigdata.littletests.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WordCountReducer extends SmartReducer<Text, LongWritable, Text, LongWritable>
{
  @Override
  protected void reduce(Text token, Iterable<LongWritable> counts, Context context) throws IOException,
      InterruptedException
  {
    long n = 0;
    for (LongWritable count : counts)
      n += count.get();
    context.write(token, new LongWritable(n));
  }
  @Override
  public Class<Text> getKeyInType()
  {
    return Text.class;
  }
  @Override
  public Class<LongWritable> getValueInType()
  {
    return LongWritable.class;
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