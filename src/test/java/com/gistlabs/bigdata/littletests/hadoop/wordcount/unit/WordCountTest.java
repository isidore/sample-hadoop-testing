package com.gistlabs.bigdata.littletests.hadoop.wordcount.unit;

import org.approvaltests.reporters.DiffReporter;
import org.approvaltests.reporters.UseReporter;
import org.junit.Test;

import com.gistlabs.bigdata.littletests.hadoop.wordcount.WordCountMapper;
import com.gistlabs.bigdata.littletests.hadoop.wordcount.WordCountReducer;

@UseReporter(DiffReporter.class)
public class WordCountTest
{
  @Test
  public void testMapper() throws Exception
  {
    HadoopApprovals.verifyMapping(new WordCountMapper(), 0, "cat cat dog");
  }
  /*
   * **************** Results For Mapper Inlined ******************
   * [cat cat dog]
   * 
   * -> maps via WordCountMapper to ->
   * 
   * (cat, 1)
   * (cat, 1)
   * (dog, 1)
   * **************** Results Inlined ******************
   */
  @Test
  public void testReducer() throws Exception
  {
    HadoopApprovals.verifyReducer(new WordCountReducer(), "Smiles", 3, 4);
  }
  /*
   * **************** Results For Reducer Inlined ******************
   * (Smiles, [3, 4])
   * 
   * -> reduces via WordCountReducer to ->
   * 
   * (Smiles, 7)
   * **************** Results Inlined ******************
   */
  @Test
  public void testMapReduce() throws Exception
  {
    HadoopApprovals.verifyMapReduce(new WordCountMapper(), new WordCountReducer(), 0, "cat cat dog");
  }
  /*
   * **************** Results For MapReduce Inlined ******************
   * [cat cat dog]
   * 
   * -> maps via WordCountMapper to ->
   * 
   * (cat, 1)
   * (cat, 1)
   * (dog, 1)
   * 
   * 
   * -> reduces via WordCountReducer to ->
   * 
   * (cat, 2)
   * (dog, 1)
   * **************** Results Inlined ******************
   */
}
