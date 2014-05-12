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
package org.apache.tez.mapreduce.processor.reduce;


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.mapreduce.TestUmbilical;
import org.apache.tez.mapreduce.TezTestUtils;
import org.apache.tez.mapreduce.hadoop.IDConverter;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.output.MROutput;
import org.apache.tez.mapreduce.output.MROutputLegacy;
import org.apache.tez.mapreduce.partition.MRPartitioner;
import org.apache.tez.mapreduce.processor.MapUtils;
import org.apache.tez.runtime.LogicalIOProcessorRuntimeTask;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.library.common.Constants;
import org.apache.tez.runtime.library.common.task.local.output.TezLocalTaskOutputFiles;
import org.apache.tez.runtime.library.common.task.local.output.TezTaskOutput;
import org.apache.tez.runtime.library.input.LocalMergedInput;
import org.apache.tez.runtime.library.output.LocalOnFileSorterOutput;
import org.apache.tez.runtime.library.shuffle.common.ShuffleUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.HashMultimap;


@SuppressWarnings("deprecation")
public class TestReduceProcessor {
  
  private static final Log LOG = LogFactory.getLog(TestReduceProcessor.class);

  private static JobConf defaultConf = new JobConf();
  private static FileSystem localFs = null;
  private static Path workDir = null;
  static {
    try {
      defaultConf.set("fs.defaultFS", "file:///");
      localFs = FileSystem.getLocal(defaultConf);
      workDir =
          new Path(new Path(System.getProperty("test.build.data", "/tmp")),
                   "TestReduceProcessor").makeQualified(localFs);
      LOG.info("Using workDir: " + workDir);
      MapUtils.configureLocalDirs(defaultConf, workDir.toString());
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  public void setUpJobConf(JobConf job) {
    job.set(TezJobConfig.LOCAL_DIRS, workDir.toString());
    job.set(MRConfig.LOCAL_DIR, workDir.toString());
    job.setClass(
        Constants.TEZ_RUNTIME_TASK_OUTPUT_MANAGER,
        TezLocalTaskOutputFiles.class, 
        TezTaskOutput.class);
    job.set(TezJobConfig.TEZ_RUNTIME_PARTITIONER_CLASS, MRPartitioner.class.getName());
    job.setNumReduceTasks(1);
  }

  @Before
  @After
  public void cleanup() throws Exception {
    localFs.delete(workDir, true);
  }

  @Test
  public void testReduceProcessor() throws Exception {
    final String dagName = "mrdag0";
    String mapVertexName = MultiStageMRConfigUtil.getInitialMapVertexName();
    String reduceVertexName = MultiStageMRConfigUtil.getFinalReduceVertexName();
    JobConf jobConf = new JobConf(defaultConf);
    setUpJobConf(jobConf);
    
    Configuration conf = MultiStageMRConfToTezTranslator.convertMRToLinearTez(jobConf);
    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, 0);
    
    Configuration mapStageConf = MultiStageMRConfigUtil.getConfForVertex(conf,
        mapVertexName);
    
    JobConf mapConf = new JobConf(mapStageConf);
    
    mapConf.set(TezJobConfig.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());
    mapConf.setBoolean(MRJobConfig.MR_TEZ_SPLITS_VIA_EVENTS, false);
    
    Path mapInput = new Path(workDir, "map0");
    MapUtils.generateInputSplit(localFs, workDir, mapConf, mapInput);
    
    InputSpec mapInputSpec = new InputSpec("NullSrcVertex",
        new InputDescriptor(MRInputLegacy.class.getName())
            .setUserPayload(MRHelpers.createMRInputPayload(mapConf, null)),
        0);
    OutputSpec mapOutputSpec = new OutputSpec("NullDestVertex", new OutputDescriptor(LocalOnFileSorterOutput.class.getName()), 1);
    // Run a map
    LogicalIOProcessorRuntimeTask mapTask = MapUtils.createLogicalTask(localFs, workDir, mapConf, 0,
        mapInput, new TestUmbilical(), dagName, mapVertexName,
        Collections.singletonList(mapInputSpec),
        Collections.singletonList(mapOutputSpec));

    mapTask.initialize();
    mapTask.run();
    mapTask.close();
    
    LOG.info("Starting reduce...");
    
    Token<JobTokenIdentifier> shuffleToken = new Token<JobTokenIdentifier>();
    
    Configuration reduceStageConf = MultiStageMRConfigUtil.getConfForVertex(conf,
        reduceVertexName);
    JobConf reduceConf = new JobConf(reduceStageConf);
    reduceConf.setOutputFormat(SequenceFileOutputFormat.class);
    reduceConf.set(TezJobConfig.TASK_LOCAL_RESOURCE_DIR, new Path(workDir,
        "localized-resources").toUri().toString());
    FileOutputFormat.setOutputPath(reduceConf, new Path(workDir, "output"));
    ProcessorDescriptor reduceProcessorDesc = new ProcessorDescriptor(
        ReduceProcessor.class.getName()).setUserPayload(TezUtils.createUserPayloadFromConf(reduceConf));
    
    InputSpec reduceInputSpec = new InputSpec(mapVertexName,
        new InputDescriptor(LocalMergedInput.class.getName()), 1);
    OutputSpec reduceOutputSpec = new OutputSpec("NullDestinationVertex",
        new OutputDescriptor(MROutputLegacy.class.getName()), 1);

    // Now run a reduce
    TaskSpec taskSpec = new TaskSpec(
        TezTestUtils.getMockTaskAttemptId(0, 1, 0, 0),
        dagName,
        reduceVertexName,
        reduceProcessorDesc,
        Collections.singletonList(reduceInputSpec),
        Collections.singletonList(reduceOutputSpec), null);

    Map<String, ByteBuffer> serviceConsumerMetadata = new HashMap<String, ByteBuffer>();
    serviceConsumerMetadata.put(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID,
        ShuffleUtils.convertJobTokenToBytes(shuffleToken));
    
    LogicalIOProcessorRuntimeTask task = new LogicalIOProcessorRuntimeTask(
        taskSpec,
        0,
        reduceConf,
        new TestUmbilical(),
        serviceConsumerMetadata,
        HashMultimap.<String, String>create());
    
    task.initialize();
    task.run();
    task.close();
    
    // MRTask mrTask = (MRTask)t.getProcessor();
    // TODO NEWTEZ Verify the partitioner has not been created
    // Likely not applicable anymore.
    // Assert.assertNull(mrTask.getPartitioner());



    // Only a task commit happens, hence the data is still in the temporary directory.
    Path reduceOutputDir = new Path(new Path(workDir, "output"),
        "_temporary/0/" + IDConverter
            .toMRTaskIdForOutput(TezTestUtils.getMockTaskId(0, 1, 0)));
    
    Path reduceOutputFile = new Path(reduceOutputDir, "part-v001-o000-00000");
    
    SequenceFile.Reader reader = new SequenceFile.Reader(localFs,
        reduceOutputFile, reduceConf);

    LongWritable key = new LongWritable();
    Text value = new Text();
    long prev = Long.MIN_VALUE;
    while (reader.next(key, value)) {
      if (prev != Long.MIN_VALUE) {
        Assert.assertTrue(prev < key.get());
        prev = key.get();
      }
    }

    reader.close();
  }

}
