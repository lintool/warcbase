/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.warcbase.mapreduce.lib;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.Stringifier;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * The Chain class provides all the common functionality for the
 * {@link TableChainMapper} and the {@link ChainReducer} classes.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class Chain {
  protected static final String CHAIN_MAPPER = "mapreduce.chain.mapper";

  protected static final String CHAIN_MAPPER_SIZE = ".size";
  protected static final String CHAIN_MAPPER_CLASS = ".mapper.class.";
  protected static final String CHAIN_MAPPER_CONFIG = ".mapper.config.";

  protected static final String MAPPER_INPUT_KEY_CLASS =
    "mapreduce.chain.mapper.input.key.class";
  protected static final String MAPPER_INPUT_VALUE_CLASS =
    "mapreduce.chain.mapper.input.value.class";
  protected static final String MAPPER_OUTPUT_KEY_CLASS =
    "mapreduce.chain.mapper.output.key.class";
  protected static final String MAPPER_OUTPUT_VALUE_CLASS =
    "mapreduce.chain.mapper.output.value.class";

  @SuppressWarnings("unchecked")
  private List<Mapper> mappers = new ArrayList<Mapper>();
  private List<Configuration> confList = new ArrayList<Configuration>();
  private Configuration rConf;
  private List<Thread> threads = new ArrayList<Thread>();
  private List<ChainBlockingQueue<?>> blockingQueues =
    new ArrayList<ChainBlockingQueue<?>>();
  private Throwable throwable = null;

  /**
   * Creates a Chain instance configured for a Mapper or a Reducer.
   */
  protected Chain() {
  }

  static class KeyValuePair<K, V> {
    K key;
    V value;
    boolean endOfInput;

    KeyValuePair(K key, V value) {
      this.key = key;
      this.value = value;
      this.endOfInput = false;
    }

    KeyValuePair(boolean eof) {
      this.key = null;
      this.value = null;
      this.endOfInput = eof;
    }
  }

  // ChainRecordReader either reads from blocking queue or task context.
  private static class ChainRecordReader<KEYIN, VALUEIN> extends
      RecordReader<KEYIN, VALUEIN> {
    private Class<?> keyClass;
    private Class<?> valueClass;
    private KEYIN key;
    private VALUEIN value;
    private Configuration conf;
    TaskInputOutputContext<KEYIN, VALUEIN, ?, ?> inputContext = null;
    ChainBlockingQueue<KeyValuePair<KEYIN, VALUEIN>> inputQueue = null;

    // constructor to read from a blocking queue
    ChainRecordReader(Class<?> keyClass, Class<?> valueClass,
        ChainBlockingQueue<KeyValuePair<KEYIN, VALUEIN>> inputQueue,
        Configuration conf) {
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      this.inputQueue = inputQueue;
      this.conf = conf;
    }

    // constructor to read from the context
    ChainRecordReader(TaskInputOutputContext<KEYIN, VALUEIN, ?, ?> context) {
      inputContext = context;
    }

    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    /**
     * Advance to the next key, value pair, returning null if at end.
     *
     * @return the key object that was read into, or null if no more
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (inputQueue != null) {
        return readFromQueue();
      } else if (inputContext.nextKeyValue()) {
        this.key = inputContext.getCurrentKey();
        this.value = inputContext.getCurrentValue();
        return true;
      } else {
        return false;
      }
    }

    @SuppressWarnings("unchecked")
    private boolean readFromQueue() throws IOException, InterruptedException {
      KeyValuePair<KEYIN, VALUEIN> kv = null;

      // wait for input on queue
      kv = inputQueue.dequeue();
      if (kv.endOfInput) {
        return false;
      }
      key = (KEYIN) ReflectionUtils.newInstance(keyClass, conf);
      value = (VALUEIN) ReflectionUtils.newInstance(valueClass, conf);
      ReflectionUtils.copy(conf, kv.key, this.key);
      ReflectionUtils.copy(conf, kv.value, this.value);
      return true;
    }

    /**
     * Get the current key.
     *
     * @return the current key object or null if there isn't one
     * @throws IOException
     * @throws InterruptedException
     */
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
      return this.key;
    }

    /**
     * Get the current value.
     *
     * @return the value object that was read into
     * @throws IOException
     * @throws InterruptedException
     */
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
      return this.value;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return 0;
    }
  }

  // ChainRecordWriter either writes to blocking queue or task context

  private static class ChainRecordWriter<KEYOUT, VALUEOUT> extends
      RecordWriter<KEYOUT, VALUEOUT> {
    TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> outputContext = null;
    ChainBlockingQueue<KeyValuePair<KEYOUT, VALUEOUT>> outputQueue = null;
    KEYOUT keyout;
    VALUEOUT valueout;
    Configuration conf;
    Class<?> keyClass;
    Class<?> valueClass;

    // constructor to write to context
    ChainRecordWriter(TaskInputOutputContext<?, ?, KEYOUT, VALUEOUT> context) {
      outputContext = context;
    }

    // constructor to write to blocking queue
    ChainRecordWriter(Class<?> keyClass, Class<?> valueClass,
        ChainBlockingQueue<KeyValuePair<KEYOUT, VALUEOUT>> output,
        Configuration conf) {
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      this.outputQueue = output;
      this.conf = conf;
    }

    /**
     * Writes a key/value pair.
     *
     * @param key
     *          the key to write.
     * @param value
     *          the value to write.
     * @throws IOException
     */
    public void write(KEYOUT key, VALUEOUT value) throws IOException,
        InterruptedException {
      if (outputQueue != null) {
        writeToQueue(key, value);
      } else {
        outputContext.write(key, value);
      }
    }

    @SuppressWarnings("unchecked")
    private void writeToQueue(KEYOUT key, VALUEOUT value) throws IOException,
        InterruptedException {
      this.keyout = (KEYOUT) ReflectionUtils.newInstance(keyClass, conf);
      this.valueout = (VALUEOUT) ReflectionUtils.newInstance(valueClass, conf);
      ReflectionUtils.copy(conf, key, this.keyout);
      ReflectionUtils.copy(conf, value, this.valueout);

      // wait to write output to queuue
      outputQueue.enqueue(new KeyValuePair<KEYOUT, VALUEOUT>(keyout, valueout));
    }

    /**
     * Close this <code>RecordWriter</code> to future operations.
     *
     * @param context
     *          the context of the task
     * @throws IOException
     */
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      if (outputQueue != null) {
        // write end of input
        outputQueue.enqueue(new KeyValuePair<KEYOUT, VALUEOUT>(true));
      }
    }

  }

  private synchronized Throwable getThrowable() {
    return throwable;
  }

  private synchronized boolean setIfUnsetThrowable(Throwable th) {
    if (throwable == null) {
      throwable = th;
      return true;
    }
    return false;
  }

  private class MapRunner<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Thread {
    private Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper;
    private Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context chainContext;
    private RecordReader<KEYIN, VALUEIN> rr;
    private RecordWriter<KEYOUT, VALUEOUT> rw;

    public MapRunner(Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapper,
        Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapperContext,
        RecordReader<KEYIN, VALUEIN> rr, RecordWriter<KEYOUT, VALUEOUT> rw)
        throws IOException, InterruptedException {
      this.mapper = mapper;
      this.rr = rr;
      this.rw = rw;
      this.chainContext = mapperContext;
    }

    @Override
    public void run() {
      if (getThrowable() != null) {
        return;
      }
      try {
        mapper.run(chainContext);
        rr.close();
        rw.close(chainContext);
      } catch (Throwable th) {
        if (setIfUnsetThrowable(th)) {
          interruptAllThreads();
        }
      }
    }
  }

  Configuration getConf(int index) {
    return confList.get(index);
  }

  /**
   * Create a map context that is based on ChainMapContext and the given record
   * reader and record writer
   */
  private <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
  Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context createMapContext(
      RecordReader<KEYIN, VALUEIN> rr, RecordWriter<KEYOUT, VALUEOUT> rw,
      TaskInputOutputContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> context,
      Configuration conf) {
    MapContext<KEYIN, VALUEIN, KEYOUT, VALUEOUT> mapContext =
      new ChainMapContextImpl<KEYIN, VALUEIN, KEYOUT, VALUEOUT>(
        context, rr, rw, conf);
    Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>.Context mapperContext =
      new WrappedMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>()
        .getMapContext(mapContext);
    return mapperContext;
  }

  @SuppressWarnings("unchecked")
  void runMapper(TaskInputOutputContext context, int index) throws IOException,
      InterruptedException {
    Mapper mapper = mappers.get(index);
    RecordReader rr = new ChainRecordReader(context);
    RecordWriter rw = new ChainRecordWriter(context);
    Mapper.Context mapperContext = createMapContext(rr, rw, context,
        getConf(index));
    mapper.run(mapperContext);
    rr.close();
    rw.close(context);
  }

  /**
   * Add mapper(the first mapper) that reads input from the input
   * context and writes to queue
   */
  @SuppressWarnings("unchecked")
  void addMapper(TaskInputOutputContext inputContext,
      ChainBlockingQueue<KeyValuePair<?, ?>> output, int index)
      throws IOException, InterruptedException {
    Configuration conf = getConf(index);
    Class<?> keyOutClass = conf.getClass(MAPPER_OUTPUT_KEY_CLASS, Object.class);
    Class<?> valueOutClass = conf.getClass(MAPPER_OUTPUT_VALUE_CLASS,
        Object.class);

    RecordReader rr = new ChainRecordReader(inputContext);
    RecordWriter rw = new ChainRecordWriter(keyOutClass, valueOutClass, output,
        conf);
    Mapper.Context mapperContext = createMapContext(rr, rw,
        (MapContext) inputContext, getConf(index));
    MapRunner runner = new MapRunner(mappers.get(index), mapperContext, rr, rw);
    threads.add(runner);
  }

  /**
   * Add mapper(the last mapper) that reads input from
   * queue and writes output to the output context
   */
  @SuppressWarnings("unchecked")
  void addMapper(ChainBlockingQueue<KeyValuePair<?, ?>> input,
      TaskInputOutputContext outputContext, int index) throws IOException,
      InterruptedException {
    Configuration conf = getConf(index);
    Class<?> keyClass = conf.getClass(MAPPER_INPUT_KEY_CLASS, Object.class);
    Class<?> valueClass = conf.getClass(MAPPER_INPUT_VALUE_CLASS, Object.class);
    RecordReader rr = new ChainRecordReader(keyClass, valueClass, input, conf);
    RecordWriter rw = new ChainRecordWriter(outputContext);
    MapRunner runner = new MapRunner(mappers.get(index), createMapContext(rr,
        rw, outputContext, getConf(index)), rr, rw);
    threads.add(runner);
  }

  /**
   * Add mapper that reads and writes from/to the queue
   */
  @SuppressWarnings("unchecked")
  void addMapper(ChainBlockingQueue<KeyValuePair<?, ?>> input,
      ChainBlockingQueue<KeyValuePair<?, ?>> output,
      TaskInputOutputContext context, int index) throws IOException,
      InterruptedException {
    Configuration conf = getConf(index);
    Class<?> keyClass = conf.getClass(MAPPER_INPUT_KEY_CLASS, Object.class);
    Class<?> valueClass = conf.getClass(MAPPER_INPUT_VALUE_CLASS, Object.class);
    Class<?> keyOutClass = conf.getClass(MAPPER_OUTPUT_KEY_CLASS, Object.class);
    Class<?> valueOutClass = conf.getClass(MAPPER_OUTPUT_VALUE_CLASS,
        Object.class);
    RecordReader rr = new ChainRecordReader(keyClass, valueClass, input, conf);
    RecordWriter rw = new ChainRecordWriter(keyOutClass, valueOutClass, output,
        conf);
    MapRunner runner = new MapRunner(mappers.get(index), createMapContext(rr,
        rw, context, getConf(index)), rr, rw);
    threads.add(runner);
  }

  // start all the threads
  void startAllThreads() {
    for (Thread thread : threads) {
      thread.start();
    }
  }

  // wait till all threads finish
  void joinAllThreads() throws IOException, InterruptedException {
    for (Thread thread : threads) {
      thread.join();
    }
    Throwable th = getThrowable();
    if (th != null) {
      if (th instanceof IOException) {
        throw (IOException) th;
      } else if (th instanceof InterruptedException) {
        throw (InterruptedException) th;
      } else {
        throw new RuntimeException(th);
      }
    }
  }

  // interrupt all threads
  private synchronized void interruptAllThreads() {
    for (Thread th : threads) {
      th.interrupt();
    }
    for (ChainBlockingQueue<?> queue : blockingQueues) {
      queue.interrupt();
    }
  }

  protected static String getPrefix() {
    return CHAIN_MAPPER;
  }

  protected static int getIndex(Configuration conf, String prefix) {
    return conf.getInt(prefix + CHAIN_MAPPER_SIZE, 0);
  }

  /**
   * Creates a {@link Configuration} for the Map or Reduce in the chain.
   *
   * <p>
   * It creates a new Configuration using the chain job's Configuration as base
   * and adds to it the configuration properties for the chain element. The keys
   * of the chain element Configuration have precedence over the given
   * Configuration.
   * </p>
   *
   * @param jobConf
   *          the chain job's Configuration.
   * @param confKey
   *          the key for chain element configuration serialized in the chain
   *          job's Configuration.
   * @return a new Configuration aggregating the chain job's Configuration with
   *         the chain element configuration properties.
   */
  protected static Configuration getChainElementConf(Configuration jobConf,
      String confKey) {
    Configuration conf = null;
    try {
      Stringifier<Configuration> stringifier =
        new DefaultStringifier<Configuration>(jobConf, Configuration.class);
      String confString = jobConf.get(confKey, null);
      if (confString != null) {
        conf = stringifier.fromString(jobConf.get(confKey, null));
      }
    } catch (IOException ioex) {
      throw new RuntimeException(ioex);
    }
    // we have to do this because the Writable desearialization clears all
    // values set in the conf making not possible do a
    // new Configuration(jobConf) in the creation of the conf above
    jobConf = new Configuration(jobConf);

    if (conf != null) {
      for (Map.Entry<String, String> entry : conf) {
        jobConf.set(entry.getKey(), entry.getValue());
      }
    }
    return jobConf;
  }

  /**
   * Adds a Mapper class to the chain job.
   *
   * <p/>
   * The configuration properties of the chain job have precedence over the
   * configuration properties of the Mapper.
   *
   * @param isMap
   *          indicates if the Chain is for a Mapper or for a Reducer.
   * @param job
   *          chain job.
   * @param klass
   *          the Mapper class to add.
   * @param inputKeyClass
   *          mapper input key class.
   * @param inputValueClass
   *          mapper input value class.
   * @param outputKeyClass
   *          mapper output key class.
   * @param outputValueClass
   *          mapper output value class.
   * @param mapperConf
   *          a configuration for the Mapper class. It is recommended to use a
   *          Configuration without default values using the
   *          <code>Configuration(boolean loadDefaults)</code> constructor with
   *          FALSE.
   */
  @SuppressWarnings("unchecked")
  protected static void addMapper(Job job,
      Class<? extends Mapper> klass, Class<?> inputKeyClass,
      Class<?> inputValueClass, Class<?> outputKeyClass,
      Class<?> outputValueClass, Configuration mapperConf) {
    String prefix = getPrefix();
    Configuration jobConf = job.getConfiguration();

    // set the mapper class
    int index = getIndex(jobConf, prefix);
    jobConf.setClass(prefix + CHAIN_MAPPER_CLASS + index, klass, Mapper.class);

    validateKeyValueTypes(jobConf, inputKeyClass, inputValueClass,
        outputKeyClass, outputValueClass, index, prefix);

    setMapperConf(jobConf, inputKeyClass, inputValueClass,
        outputKeyClass, outputValueClass, mapperConf, index, prefix);
  }

  protected static void validateKeyValueTypes(
      Configuration jobConf, Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass, int index,
      String prefix) {
    if (index > 0) {
      // check the that the new Mapper in the chain key and value input classes
      // match those of the previous Mapper output.
      Configuration previousMapperConf = getChainElementConf(jobConf, prefix
          + CHAIN_MAPPER_CONFIG + (index - 1));
      if (!inputKeyClass.isAssignableFrom(previousMapperConf.getClass(
          MAPPER_OUTPUT_KEY_CLASS, null))) {
        throw new IllegalArgumentException("The specified Mapper input key class does"
            + " not match the previous Mapper's output key class.");
      }
      if (!inputValueClass.isAssignableFrom(previousMapperConf.getClass(
          MAPPER_OUTPUT_VALUE_CLASS, null))) {
        throw new IllegalArgumentException("The specified Mapper input value class"
            + " does not match the previous Mapper's output value class.");
      }
    }
  }

  protected static void setMapperConf(Configuration jobConf,
      Class<?> inputKeyClass, Class<?> inputValueClass,
      Class<?> outputKeyClass, Class<?> outputValueClass,
      Configuration mapperConf, int index, String prefix) {
    // if the Mapper does not have a configuration, create an empty one
    if (mapperConf == null) {
      // using a Configuration without defaults to make it lightweight.
      // still the chain's conf may have all defaults and this conf is
      // overlapped to the chain configuration one.
      mapperConf = new Configuration(true);
    }

    // store the input/output classes of the mapper in the mapper conf
    mapperConf.setClass(MAPPER_INPUT_KEY_CLASS, inputKeyClass, Object.class);
    mapperConf
        .setClass(MAPPER_INPUT_VALUE_CLASS, inputValueClass, Object.class);
    mapperConf.setClass(MAPPER_OUTPUT_KEY_CLASS, outputKeyClass, Object.class);
    mapperConf.setClass(MAPPER_OUTPUT_VALUE_CLASS, outputValueClass,
        Object.class);
    // serialize the mapper configuration in the chain configuration.
    Stringifier<Configuration> stringifier =
      new DefaultStringifier<Configuration>(jobConf, Configuration.class);
    try {
      jobConf.set(prefix + CHAIN_MAPPER_CONFIG + index, stringifier
          .toString(new Configuration(mapperConf)));
    } catch (IOException ioEx) {
      throw new RuntimeException(ioEx);
    }

    // increment the chain counter
    jobConf.setInt(prefix + CHAIN_MAPPER_SIZE, index + 1);
  }

  /**
   * Setup the chain.
   *
   * @param jobConf
   *          chain job's {@link Configuration}.
   */
  @SuppressWarnings("unchecked")
  void setup(Configuration jobConf) {
    String prefix = getPrefix();

    int index = jobConf.getInt(prefix + CHAIN_MAPPER_SIZE, 0);
    for (int i = 0; i < index; i++) {
      Class<? extends Mapper> klass = jobConf.getClass(prefix
          + CHAIN_MAPPER_CLASS + i, null, Mapper.class);
      Configuration mConf = getChainElementConf(jobConf, prefix
          + CHAIN_MAPPER_CONFIG + i);
      confList.add(mConf);
      Mapper mapper = ReflectionUtils.newInstance(klass, mConf);
      mappers.add(mapper);

    }
  }

  @SuppressWarnings("unchecked")
  List<Mapper> getAllMappers() {
    return mappers;
  }

  /**
   * Creates a ChainBlockingQueue with KeyValuePair as element
   *
   * @return the ChainBlockingQueue
   */
  ChainBlockingQueue<KeyValuePair<?, ?>> createBlockingQueue() {
    return new ChainBlockingQueue<KeyValuePair<?, ?>>();
  }

  /**
   * A blocking queue with one element.
   *
   * @param <E>
   */
  class ChainBlockingQueue<E> {
    E element = null;
    boolean isInterrupted = false;

    ChainBlockingQueue() {
      blockingQueues.add(this);
    }

    synchronized void enqueue(E e) throws InterruptedException {
      while (element != null) {
        if (isInterrupted) {
          throw new InterruptedException();
        }
        this.wait();
      }
      element = e;
      this.notify();
    }

    synchronized E dequeue() throws InterruptedException {
      while (element == null) {
        if (isInterrupted) {
          throw new InterruptedException();
        }
        this.wait();
      }
      E e = element;
      element = null;
      this.notify();
      return e;
    }

    synchronized void interrupt() {
      isInterrupted = true;
      this.notifyAll();
    }
  }
}
