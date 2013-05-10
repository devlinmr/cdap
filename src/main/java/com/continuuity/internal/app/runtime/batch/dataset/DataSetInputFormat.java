package com.continuuity.internal.app.runtime.batch.dataset;

import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.batch.BatchReadable;
import com.continuuity.api.data.batch.Split;
import com.continuuity.api.data.batch.SplitReader;
import com.continuuity.internal.app.runtime.batch.BasicMapReduceContext;
import com.continuuity.internal.app.runtime.batch.MapReduceContextProvider;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public final class DataSetInputFormat<KEY, VALUE> extends InputFormat<KEY, VALUE> {
  public static final String INPUT_DATASET_SPEC = "input.dataset.spec";

  public static void setInput(Job job, DataSet dataSet) {
    job.setInputFormatClass(DataSetInputFormat.class);
    job.getConfiguration().set(DataSetInputFormat.INPUT_DATASET_SPEC, new Gson().toJson(dataSet.configure()));
  }

  @Override
  public List<InputSplit> getSplits(final JobContext context) throws IOException, InterruptedException {
    MapReduceContextProvider contextProvider = new MapReduceContextProvider(context);
    List<Split> splits = contextProvider.get().getInputDataSelection();

    List<InputSplit> list = new ArrayList<InputSplit>();
    for (Split split : splits) {
      list.add(new DataSetInputSplit(split));
    }

    return list;
  }

  @Override
  public RecordReader<KEY, VALUE> createRecordReader(final InputSplit split,
                                                         final TaskAttemptContext context)
    throws IOException, InterruptedException {

    DataSetInputSplit inputSplit = (DataSetInputSplit) split;

    Configuration conf = context.getConfiguration();
    MapReduceContextProvider contextProvider = new MapReduceContextProvider(context);
    BasicMapReduceContext mrContext = contextProvider.get();
    @SuppressWarnings("unchecked")
    BatchReadable<KEY, VALUE> dataset = (BatchReadable) mrContext.getDataSet(getInputDataSetSpec(conf).getName());
    SplitReader<KEY, VALUE> splitReader = dataset.createSplitReader(inputSplit.getSplit());

    return new DataSetRecordReader<KEY, VALUE>(dataset, splitReader, mrContext);
  }

  private DataSetSpecification getInputDataSetSpec(Configuration conf) {
    return new Gson().fromJson(conf.get(INPUT_DATASET_SPEC), DataSetSpecification.class);
  }
}
