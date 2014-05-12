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
package org.apache.tez.runtime.library.output;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.events.CompositeDataMovementEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.KeyValueWriter;
import org.apache.tez.runtime.library.common.MemoryUpdateCallbackHandler;
import org.apache.tez.runtime.library.common.sort.impl.ExternalSorter;
import org.apache.tez.runtime.library.common.sort.impl.PipelinedSorter;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;
import org.apache.tez.runtime.library.common.sort.impl.dflt.DefaultSorter;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.VertexManagerEventPayloadProto;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;

/**
 * <code>OnFileSortedOutput</code> is an {@link LogicalOutput} which sorts key/value pairs 
 * written to it and persists it to a file.
 */
public class OnFileSortedOutput implements LogicalOutput {

  private static final Log LOG = LogFactory.getLog(OnFileSortedOutput.class);

  protected ExternalSorter sorter;
  protected Configuration conf;
  protected int numOutputs;
  protected TezOutputContext outputContext;
  protected MemoryUpdateCallbackHandler memoryUpdateCallbackHandler;
  private long startTime;
  private long endTime;
  private boolean sendEmptyPartitionDetails;
  private final AtomicBoolean isStarted = new AtomicBoolean(false);

  @Override
  public synchronized List<Event> initialize(TezOutputContext outputContext)
      throws IOException {
    this.startTime = System.nanoTime();
    this.outputContext = outputContext;
    this.conf = TezUtils.createConfFromUserPayload(outputContext.getUserPayload());
    // Initializing this parametr in this conf since it is used in multiple
    // places (wherever LocalDirAllocator is used) - TezTaskOutputFiles,
    // TezMerger, etc.
    this.conf.setStrings(TezJobConfig.LOCAL_DIRS, outputContext.getWorkDirs());
    this.memoryUpdateCallbackHandler = new MemoryUpdateCallbackHandler();
    outputContext.requestInitialMemory(
        ExternalSorter.getInitialMemoryRequirement(conf,
            outputContext.getTotalMemoryAvailableToTask()), memoryUpdateCallbackHandler);

    sendEmptyPartitionDetails = this.conf.getBoolean(
        TezJobConfig.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED,
        TezJobConfig.TEZ_RUNTIME_EMPTY_PARTITION_INFO_VIA_EVENTS_ENABLED_DEFAULT);
    return Collections.emptyList();
  }

  @Override
  public synchronized void start() throws Exception {
    if (!isStarted.get()) {
      memoryUpdateCallbackHandler.validateUpdateReceived();
      if (this.conf.getInt(TezJobConfig.TEZ_RUNTIME_SORT_THREADS,
          TezJobConfig.DEFAULT_TEZ_RUNTIME_SORT_THREADS) > 1) {
        sorter = new PipelinedSorter(outputContext, conf, numOutputs,
            memoryUpdateCallbackHandler.getMemoryAssigned());
      } else {
        sorter = new DefaultSorter(outputContext, conf, numOutputs,
            memoryUpdateCallbackHandler.getMemoryAssigned());
      }
      isStarted.set(true);
    }
  }

  @Override
  public synchronized KeyValueWriter getWriter() throws IOException {
    Preconditions.checkState(isStarted.get(), "Cannot get writer before starting the Output");
    return new KeyValueWriter() {
      @Override
      public void write(Object key, Object value) throws IOException {
        sorter.write(key, value);
      }
    };
  }

  @Override
  public synchronized void handleEvents(List<Event> outputEvents) {
    // Not expecting any events.
  }

  @Override
  public synchronized void setNumPhysicalOutputs(int numOutputs) {
    this.numOutputs = numOutputs;
  }

  @Override
  public synchronized List<Event> close() throws IOException {
    if (sorter != null) {
      sorter.flush();
      sorter.close();
      this.endTime = System.nanoTime();
      return generateEventsOnClose();
    } else {
      LOG.warn("Attempting to close output " + outputContext.getDestinationVertexName()
          + " before it was started");
      return Collections.emptyList();
    }
  }
  
  protected List<Event> generateEventsOnClose() throws IOException {
    String host = System.getenv(ApplicationConstants.Environment.NM_HOST
        .toString());
    ByteBuffer shuffleMetadata = outputContext
        .getServiceProviderMetaData(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID);
    int shufflePort = ShuffleUtils.deserializeShuffleProviderMetaData(shuffleMetadata);

    DataMovementEventPayloadProto.Builder payloadBuilder = DataMovementEventPayloadProto
        .newBuilder();

    if (sendEmptyPartitionDetails) {
      Path indexFile = sorter.getMapOutput().getOutputIndexFile();
      TezSpillRecord spillRecord = new TezSpillRecord(indexFile, conf);
      BitSet emptyPartitionDetails = new BitSet();
      int emptyPartitions = 0;
      for(int i=0;i<spillRecord.size();i++) {
        TezIndexRecord indexRecord = spillRecord.getIndex(i);
        if (!indexRecord.hasData()) {
          emptyPartitionDetails.set(i);
          emptyPartitions++;
        }
      }
      if (emptyPartitions > 0) {
        ByteString emptyPartitionsBytesString =
            TezUtils.compressByteArrayToByteString(TezUtils.toByteArray(emptyPartitionDetails));
        payloadBuilder.setEmptyPartitions(emptyPartitionsBytesString);
        LOG.info("EmptyPartition bitsetSize=" + emptyPartitionDetails.cardinality() + ", numOutputs="
                + numOutputs + ", emptyPartitions=" + emptyPartitions
              + ", compressedSize=" + emptyPartitionsBytesString.size());
      }
    }
    payloadBuilder.setHost(host);
    payloadBuilder.setPort(shufflePort);
    payloadBuilder.setPathComponent(outputContext.getUniqueIdentifier());
    payloadBuilder.setRunDuration((int) ((endTime - startTime) / 1000));
    DataMovementEventPayloadProto payloadProto = payloadBuilder.build();
    byte[] payloadBytes = payloadProto.toByteArray();

    long outputSize = outputContext.getCounters()
        .findCounter(TaskCounter.OUTPUT_BYTES).getValue();
    VertexManagerEventPayloadProto.Builder vmBuilder = VertexManagerEventPayloadProto
        .newBuilder();
    vmBuilder.setOutputSize(outputSize);
    VertexManagerEvent vmEvent = new VertexManagerEvent(
        outputContext.getDestinationVertexName(), vmBuilder.build().toByteArray());    

    List<Event> events = Lists.newArrayListWithCapacity(numOutputs+1);
    events.add(vmEvent);

    CompositeDataMovementEvent csdme = new CompositeDataMovementEvent(0, numOutputs, payloadBytes);
    events.add(csdme);

    return events;
  }
}
