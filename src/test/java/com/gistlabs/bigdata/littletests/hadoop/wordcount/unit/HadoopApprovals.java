package com.gistlabs.bigdata.littletests.hadoop.wordcount.unit;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.approvaltests.Approvals;
import org.lambda.functions.Function1;

import com.gistlabs.bigdata.littletests.hadoop.wordcount.SmartMapper;
import com.gistlabs.bigdata.littletests.hadoop.wordcount.SmartReducer;
import com.spun.util.ArrayUtils;

@SuppressWarnings({"rawtypes", "unchecked"})
public class HadoopApprovals
{
  public static void verifyMapping(SmartMapper mapper, Object key, Object input) throws Exception
  {
    MapDriver mapDriver = new MapDriver();
    mapDriver.setMapper(mapper);
    Object writableKey = createWritable(key, mapper.getKeyInType());
    Object writableValue = createWritable(input, mapper.getValueInType());
    mapDriver.withInput(writableKey, writableValue);
    List results = mapDriver.run();
    Collections.sort(results, PairComparer.INSTANCE);
    String header = String.format("[%s]\r\n\r\n -> maps via %s to -> \r\n", input, mapper.getClass()
        .getSimpleName());
    Approvals.verifyAll(header, results, Echo.INSTANCE);
  }
  private static class PairComparer implements Comparator<Pair>
  {
    public static final PairComparer INSTANCE = new PairComparer();
    public int compare(Pair o1, Pair o2)
    {
      return ((Comparable) o1.getFirst()).compareTo(o2.getFirst());
    }
  }
  private static class Echo implements Function1<Object, String>
  {
    public static final Echo INSTANCE = new Echo();
    @Override
    public String call(Object in)
    {
      return "" + in;
    }
  }
  public static void verifyReducer(SmartReducer reducer, Object key, Object... values) throws Exception
  {
    List list = new ArrayList();
    for (Object value : values)
    {
      list.add(createWritable(value, reducer.getValueInType()));
    }
    ReduceDriver reduceDriver = new ReduceDriver<Text, LongWritable, Text, LongWritable>();
    reduceDriver.withInput(createWritable(key, reducer.getKeyInType()), list);
    reduceDriver.setReducer(reducer);
    List results = reduceDriver.run();
    Collections.sort(results, PairComparer.INSTANCE);
    String header = String.format("(%s, %s)\r\n\r\n -> reduces via %s to -> \r\n", key, list, reducer.getClass()
        .getSimpleName());
    Approvals.verifyAll(header, results, Echo.INSTANCE);
  }
  public static Object createWritable(Object value, Class writableType) throws InstantiationException,
      IllegalAccessException, InvocationTargetException, NoSuchMethodException
  {
    Object writable = writableType.newInstance();
    try
    {
      Method method = writableType.getMethod("set", value.getClass());
      method.invoke(writable, value);
      return writable;
    }
    catch (Exception e)
    {
      // TODO: handle exception
    }
    for (Method m : writableType.getMethods())
    {
      if (m.getName().equals("set") && m.getParameterTypes().length == 1) // needed for string
      {
        m.invoke(writable, value);
      }
    }
    return writable;
  }
  public static void verifyMapReduce(SmartMapper mapper, SmartReducer reducer, Object key, Object input)
      throws Exception
  {
    MapDriver mapDriver = new MapDriver();
    mapDriver.setMapper(mapper);
    MapReduceDriver mapReduceDriver = new MapReduceDriver();
    mapReduceDriver.setMapper(mapper);
    Object writableKey = createWritable(key, mapper.getKeyInType());
    Object writableValue = createWritable(input, mapper.getValueInType());
    mapDriver.withInput(writableKey, writableValue);
    List results = mapDriver.run();
    Collections.sort(results, PairComparer.INSTANCE);
    mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, LongWritable, Text, LongWritable>();
    writableKey = createWritable(key, mapper.getKeyInType());
    writableValue = createWritable(input, mapper.getValueInType());
    mapReduceDriver.withInput(writableKey, writableValue);
    mapReduceDriver.setMapper(mapper);
    mapReduceDriver.setReducer(reducer);
    List finalResults = mapReduceDriver.run();
    String text = String.format(
        "[%s]\r\n\r\n -> maps via %s to -> \r\n\r\n%s\r\n\r\n -> reduces via %s to -> \r\n\r\n%s", input, mapper
            .getClass().getSimpleName(), ArrayUtils.toString(results, Echo.INSTANCE), reducer.getClass()
            .getSimpleName(), ArrayUtils.toString(finalResults, Echo.INSTANCE));
    Approvals.verify(text);
  }
}
