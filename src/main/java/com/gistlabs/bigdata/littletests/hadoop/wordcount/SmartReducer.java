package com.gistlabs.bigdata.littletests.hadoop.wordcount;

import org.apache.hadoop.mapreduce.Reducer;

public abstract class SmartReducer<KeyIn, ValueIn, KeyOut, ValueOut>
    extends
      Reducer<KeyIn, ValueIn, KeyOut, ValueOut> implements Transform<KeyIn, ValueIn, KeyOut, ValueOut>
{
}