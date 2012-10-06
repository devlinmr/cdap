package com.continuuity.data.operation.executor.simple;

import com.continuuity.api.data.*;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.NoOperationExecutor;
import com.continuuity.data.operation.ttqueue.QueueEnqueue;
import com.continuuity.data.operation.ttqueue.TTQueueTable;
import com.continuuity.data.table.ColumnarTable;
import com.continuuity.data.table.ColumnarTableHandle;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;

public class SimpleOperationExecutor extends NoOperationExecutor {

  final ColumnarTableHandle tableHandle;
  final ColumnarTable randomTable;
  final ColumnarTable orderedTable;
  final TTQueueTable queueTable;

  public SimpleOperationExecutor(ColumnarTableHandle tableHandle)
      throws OperationException {
    this.tableHandle = tableHandle;
    this.randomTable = tableHandle.getTable(Bytes.toBytes("random"));
    this.orderedTable = tableHandle.getTable(Bytes.toBytes("ordered"));
    this.queueTable = tableHandle.getQueueTable(Bytes.toBytes("queues"));
  }

  // Batch of Writes

  @Override
  public void execute(OperationContext context,
                      List<WriteOperation> writes) throws OperationException {
    for (WriteOperation write : writes) {
      if (write instanceof Write)
        execute(context, (Write)write);
      else if (write instanceof Increment)
        execute(context, (Increment)write);
      else if (write instanceof Delete)
        execute(context, (Delete)write);
      else if (write instanceof CompareAndSwap)
        execute(context, (CompareAndSwap)write);
      else if (write instanceof QueueEnqueue)
        execute(context, (QueueEnqueue)write);
      else throw new OperationException(StatusCode.INTERNAL_ERROR,
            "Unknown write operation " + write.getClass().getName());
    }
  }

  // Single Writes

  @Override
  public void execute(OperationContext context,
                      Write write) throws OperationException {
    this.randomTable.put(write.getKey(), write.getColumns(), write.getValues());
  }

  @Override
  public void execute(OperationContext context,
                      Delete delete) throws OperationException {
    this.randomTable.delete(delete.getKey(), delete.getColumns()[0]);
  }

  // Conditional Writes
  @Override
  public void execute(OperationContext context,
                      CompareAndSwap cas) throws OperationException {
    this.randomTable.compareAndSwap(cas.getKey(), Operation.KV_COL,
        cas.getExpectedValue(), cas.getNewValue());
  }

  // Value Returning Read-Modify-Writes
  @Override
  public void execute(OperationContext context,
                      Increment inc) throws OperationException {
    this.randomTable.increment(
        inc.getKey(), inc.getColumns()[0], inc.getAmounts()[0]);
  }

  // Simple Reads
  @Override
  public OperationResult<byte[]> execute(OperationContext context,
                                         ReadKey read)
      throws OperationException {
    return this.randomTable.get(read.getKey(), Operation.KV_COL);
  }

  @Override
  public String getName() {
    return "simple";
  }
}
