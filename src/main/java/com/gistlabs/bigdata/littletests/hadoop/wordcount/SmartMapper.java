package com.gistlabs.bigdata.littletests.hadoop.wordcount;

import org.apache.hadoop.mapreduce.Mapper;

public abstract class SmartMapper<KeyIn, ValueIn, KeyOut, ValueOut>
    extends
      Mapper<KeyIn, ValueIn, KeyOut, ValueOut> implements Transform<KeyIn, ValueIn, KeyOut, ValueOut>
{
}
