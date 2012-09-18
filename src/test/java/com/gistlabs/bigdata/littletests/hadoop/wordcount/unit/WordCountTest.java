package com.gistlabs.bigdata.littletests.hadoop.wordcount.unit;

import org.approvaltests.reporters.DiffReporter;
import org.approvaltests.reporters.UseReporter;
import org.junit.Test;

import com.gistlabs.bigdata.littletests.hadoop.wordcount.WordCountMapper;
import com.gistlabs.bigdata.littletests.hadoop.wordcount.WordCountReducer;

@UseReporter(DiffReporter.class)
public class WordCountTest// extends AbstractWordCount
{
  @Test
  public void testMapper() throws Exception
  {
    HadoopApprovals.verifyMapping(new WordCountMapper(), 0, "cat cat dog");
  }
  @Test
  public void testReducer() throws Exception
  {
    HadoopApprovals.verifyReducer(new WordCountReducer(), "Smiles", 3, 4);
  }
  @Test
  public void testMapReduce() throws Exception
  {
    HadoopApprovals.verifyMapReduce(new WordCountMapper(), new WordCountReducer(), 0, "cat cat dog");
  }
}
