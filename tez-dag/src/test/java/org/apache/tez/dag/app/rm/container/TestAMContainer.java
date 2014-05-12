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

package org.apache.tez.dag.app.rm.container;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.TokenCache;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventContainerTerminated;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventNodeFailed;
import org.apache.tez.dag.app.dag.event.TaskAttemptEventType;
import org.apache.tez.dag.app.rm.AMSchedulerEventType;
import org.apache.tez.dag.app.rm.NMCommunicatorEventType;
import org.apache.tez.dag.app.rm.NMCommunicatorLaunchRequestEvent;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Maps;


public class TestAMContainer {


  @Test
  // Assign before launch.
  public void tetSingleSuccessfulTaskFlow() {
    WrappedContainer wc = new WrappedContainer();

    wc.verifyState(AMContainerState.ALLOCATED);

    // Launch request.
    wc.launchContainer();
    wc.verifyState(AMContainerState.LAUNCHING);
    // 1 Launch request.
    wc.verifyCountAndGetOutgoingEvents(1);

    // Assign task.
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.verifyState(AMContainerState.LAUNCHING);
    wc.verifyNoOutgoingEvents();
    assertEquals(wc.taskAttemptID, wc.amContainer.getQueuedTaskAttempts()
        .get(0));

    // Container Launched
    wc.containerLaunched();
    wc.verifyState(AMContainerState.IDLE);
    wc.verifyNoOutgoingEvents();
    assertEquals(wc.taskAttemptID, wc.amContainer.getQueuedTaskAttempts()
        .get(0));
    assertNull(wc.amContainer.getRunningTaskAttempt());
    verify(wc.tal).registerRunningContainer(wc.containerID);
    verify(wc.chh).register(wc.containerID);

    // Pull TA
    AMContainerTask pulledTask = wc.pullTaskToRun();
    wc.verifyState(AMContainerState.RUNNING);
    wc.verifyNoOutgoingEvents();
    assertFalse(pulledTask.shouldDie());
    assertEquals(wc.taskSpec.getTaskAttemptID(), pulledTask.getTask()
        .getTaskAttemptID());
    assertEquals(wc.taskAttemptID, wc.amContainer.getRunningTaskAttempt());
    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());

    wc.taskAttemptSucceeded(wc.taskAttemptID);
    wc.verifyState(AMContainerState.IDLE);
    wc.verifyNoOutgoingEvents();
    assertNull(wc.amContainer.getRunningTaskAttempt());
    verify(wc.tal).unregisterTaskAttempt(wc.taskAttemptID);

    wc.containerCompleted(false);
    wc.verifyState(AMContainerState.COMPLETED);
    wc.verifyNoOutgoingEvents();
    verify(wc.tal).unregisterRunningContainer(wc.containerID);
    verify(wc.chh).unregister(wc.containerID);

    assertEquals(1, wc.amContainer.getAllTaskAttempts().size());
    assertFalse(wc.amContainer.isInErrorState());
  }

  @Test
  // Assign after launch.
  public void testSingleSuccessfulTaskFlow2() {
    WrappedContainer wc = new WrappedContainer();

    wc.verifyState(AMContainerState.ALLOCATED);

    // Launch request.
    wc.launchContainer();
    wc.verifyState(AMContainerState.LAUNCHING);
    // 1 Launch request.
    wc.verifyCountAndGetOutgoingEvents(1);

    // Container Launched
    wc.containerLaunched();
    wc.verifyState(AMContainerState.IDLE);
    wc.verifyNoOutgoingEvents();
    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());
    verify(wc.tal).registerRunningContainer(wc.containerID);
    verify(wc.chh).register(wc.containerID);

    // Assign task.
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.verifyState(AMContainerState.IDLE);
    wc.verifyNoOutgoingEvents();
    assertEquals(wc.taskAttemptID, wc.amContainer.getQueuedTaskAttempts()
        .get(0));
    assertNull(wc.amContainer.getRunningTaskAttempt());

    // Pull TA
    AMContainerTask pulledTask = wc.pullTaskToRun();
    wc.verifyState(AMContainerState.RUNNING);
    wc.verifyNoOutgoingEvents();
    assertFalse(pulledTask.shouldDie());
    assertEquals(wc.taskSpec.getTaskAttemptID(), pulledTask.getTask()
        .getTaskAttemptID());
    assertEquals(wc.taskAttemptID, wc.amContainer.getRunningTaskAttempt());
    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());

    wc.taskAttemptSucceeded(wc.taskAttemptID);
    wc.verifyState(AMContainerState.IDLE);
    wc.verifyNoOutgoingEvents();
    assertNull(wc.amContainer.getRunningTaskAttempt());
    verify(wc.tal).unregisterTaskAttempt(wc.taskAttemptID);

    wc.containerCompleted(false);
    wc.verifyState(AMContainerState.COMPLETED);
    wc.verifyNoOutgoingEvents();
    verify(wc.tal).unregisterRunningContainer(wc.containerID);
    verify(wc.chh).unregister(wc.containerID);

    assertEquals(1, wc.amContainer.getAllTaskAttempts().size());
    assertFalse(wc.amContainer.isInErrorState());
  }

  @Test
  public void testSingleSuccessfulTaskFlowStopRequest() {
    WrappedContainer wc = new WrappedContainer();

    wc.verifyState(AMContainerState.ALLOCATED);

    wc.launchContainer();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.containerLaunched();
    wc.pullTaskToRun();
    wc.taskAttemptSucceeded(wc.taskAttemptID);

    wc.stopRequest();
    wc.verifyState(AMContainerState.STOP_REQUESTED);
    // Event to NM to stop the container.
    wc.verifyCountAndGetOutgoingEvents(1);
    assertTrue(wc.verifyCountAndGetOutgoingEvents(1).get(0).getType() ==
        NMCommunicatorEventType.CONTAINER_STOP_REQUEST);

    wc.nmStopSent();
    wc.verifyState(AMContainerState.STOPPING);
    wc.verifyNoOutgoingEvents();

    wc.containerCompleted(false);
    wc.verifyState(AMContainerState.COMPLETED);
    wc.verifyNoOutgoingEvents();
    verify(wc.tal).unregisterRunningContainer(wc.containerID);
    verify(wc.chh).unregister(wc.containerID);

    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());
    assertNull(wc.amContainer.getRunningTaskAttempt());
    assertEquals(1, wc.amContainer.getAllTaskAttempts().size());
    assertFalse(wc.amContainer.isInErrorState());
  }

  @Test
  public void testSingleSuccessfulTaskFlowFailedNMStopRequest() {
    WrappedContainer wc = new WrappedContainer();

    wc.verifyState(AMContainerState.ALLOCATED);

    wc.launchContainer();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.containerLaunched();
    wc.pullTaskToRun();
    wc.taskAttemptSucceeded(wc.taskAttemptID);

    wc.stopRequest();
    wc.verifyState(AMContainerState.STOP_REQUESTED);
    // Event to NM to stop the container.
    wc.verifyCountAndGetOutgoingEvents(1);
    assertTrue(wc.verifyCountAndGetOutgoingEvents(1).get(0).getType() ==
        NMCommunicatorEventType.CONTAINER_STOP_REQUEST);

    wc.nmStopFailed();
    wc.verifyState(AMContainerState.STOPPING);
    // Event to ask a RM container release.
    wc.verifyCountAndGetOutgoingEvents(1);
    assertTrue(wc.verifyCountAndGetOutgoingEvents(1).get(0).getType() ==
        AMSchedulerEventType.S_CONTAINER_DEALLOCATE);

    wc.containerCompleted(false);
    wc.verifyState(AMContainerState.COMPLETED);
    wc.verifyNoOutgoingEvents();
    verify(wc.tal).unregisterRunningContainer(wc.containerID);
    verify(wc.chh).unregister(wc.containerID);

    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());
    assertNull(wc.amContainer.getRunningTaskAttempt());
    assertEquals(1, wc.amContainer.getAllTaskAttempts().size());
    assertFalse(wc.amContainer.isInErrorState());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testMultipleAllocationsAtIdle() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();
    wc.containerLaunched();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.verifyState(AMContainerState.IDLE);

    TezTaskAttemptID taID2 = TezTaskAttemptID.getInstance(wc.taskID, 2);
    wc.assignTaskAttempt(taID2);

    wc.verifyState(AMContainerState.STOP_REQUESTED);
    verify(wc.tal).unregisterRunningContainer(wc.containerID);
    verify(wc.chh).unregister(wc.containerID);
    // 1 for NM stop request. 2 TERMINATING to TaskAttempt.
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(3);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        NMCommunicatorEventType.CONTAINER_STOP_REQUEST,
        TaskAttemptEventType.TA_CONTAINER_TERMINATING,
        TaskAttemptEventType.TA_CONTAINER_TERMINATING);
    assertTrue(wc.amContainer.isInErrorState());

    wc.nmStopSent();
    wc.containerCompleted(false);
    // 1 Inform scheduler. 2 TERMINATED to TaskAttempt.
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(2);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED);

    assertNull(wc.amContainer.getRunningTaskAttempt());
    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());
    assertEquals(2, wc.amContainer.getAllTaskAttempts().size());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testAllocationAtRunning() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();
    wc.containerLaunched();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.pullTaskToRun();
    wc.verifyState(AMContainerState.RUNNING);

    TezTaskAttemptID taID2 = TezTaskAttemptID.getInstance(wc.taskID, 2);
    wc.assignTaskAttempt(taID2);

    wc.verifyState(AMContainerState.STOP_REQUESTED);
    verify(wc.tal).unregisterRunningContainer(wc.containerID);
    verify(wc.chh).unregister(wc.containerID);
    // 1 for NM stop request. 2 TERMINATING to TaskAttempt.
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(3);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        NMCommunicatorEventType.CONTAINER_STOP_REQUEST,
        TaskAttemptEventType.TA_CONTAINER_TERMINATING,
        TaskAttemptEventType.TA_CONTAINER_TERMINATING);
    assertTrue(wc.amContainer.isInErrorState());

    wc.nmStopSent();
    wc.containerCompleted(false);
    // 1 Inform scheduler. 2 TERMINATED to TaskAttempt.
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(2);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED);

    assertNull(wc.amContainer.getRunningTaskAttempt());
    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());
    assertEquals(2, wc.amContainer.getAllTaskAttempts().size());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testMultipleAllocationsAtLaunching() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.pullTaskToRun();
    wc.verifyState(AMContainerState.LAUNCHING);

    TezTaskAttemptID taID2 = TezTaskAttemptID.getInstance(wc.taskID, 2);
    wc.assignTaskAttempt(taID2);

    wc.verifyState(AMContainerState.STOP_REQUESTED);
    verify(wc.tal).unregisterRunningContainer(wc.containerID);
    verify(wc.chh).unregister(wc.containerID);
    // 1 for NM stop request. 2 TERMINATING to TaskAttempt.
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(3);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        NMCommunicatorEventType.CONTAINER_STOP_REQUEST,
        TaskAttemptEventType.TA_CONTAINER_TERMINATING,
        TaskAttemptEventType.TA_CONTAINER_TERMINATING);
    assertTrue(wc.amContainer.isInErrorState());

    wc.nmStopSent();
    wc.containerCompleted(false);
    // 1 Inform scheduler. 2 TERMINATED to TaskAttempt.
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(2);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED);

    assertNull(wc.amContainer.getRunningTaskAttempt());
    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());
    assertEquals(2, wc.amContainer.getAllTaskAttempts().size());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testContainerTimedOutAtRunning() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();
    wc.containerLaunched();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.pullTaskToRun();
    wc.verifyState(AMContainerState.RUNNING);

    wc.containerTimedOut();
    wc.verifyState(AMContainerState.STOP_REQUESTED);
    verify(wc.tal).unregisterRunningContainer(wc.containerID);
    verify(wc.chh).unregister(wc.containerID);
    // 1 to TA, 1 for RM de-allocate.
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(2);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATING,
        NMCommunicatorEventType.CONTAINER_STOP_REQUEST);
    // TODO Should this be an RM DE-ALLOCATE instead ?

    wc.containerCompleted(false);
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(1);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED);

    assertFalse(wc.amContainer.isInErrorState());

    assertNull(wc.amContainer.getRunningTaskAttempt());
    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());
    assertEquals(1, wc.amContainer.getAllTaskAttempts().size());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testLaunchFailure() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.verifyState(AMContainerState.LAUNCHING);
    wc.launchFailed();
    wc.verifyState(AMContainerState.STOPPING);
    verify(wc.tal).registerRunningContainer(wc.containerID);
    verify(wc.tal).unregisterRunningContainer(wc.containerID);

    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(2);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATING,
        AMSchedulerEventType.S_CONTAINER_DEALLOCATE);

    wc.containerCompleted(false);
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(1);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED);

    // Valid transition. Container complete, but not with an error.
    assertFalse(wc.amContainer.isInErrorState());
  }

  @Test
  public void testContainerCompletedAtAllocated() {
    WrappedContainer wc = new WrappedContainer();
    wc.verifyState(AMContainerState.ALLOCATED);

    wc.containerCompleted(false);
    wc.verifyState(AMContainerState.COMPLETED);
    wc.verifyNoOutgoingEvents();

    assertFalse(wc.amContainer.isInErrorState());
  }

  @SuppressWarnings("rawtypes")
  @Test
  // Verify that incoming NM launched events to COMPLETED containers are
  // handled.
  public void testContainerCompletedAtLaunching() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();


    wc.assignTaskAttempt(wc.taskAttemptID);

    wc.containerCompleted(false);
    wc.verifyState(AMContainerState.COMPLETED);
    verify(wc.tal).registerRunningContainer(wc.containerID);
    verify(wc.tal).unregisterRunningContainer(wc.containerID);

    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(1);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED);

    assertFalse(wc.amContainer.isInErrorState());

    // Container launched generated by NM call.
    wc.containerLaunched();
    wc.verifyNoOutgoingEvents();

    assertFalse(wc.amContainer.isInErrorState());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testContainerCompletedAtIdle() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();

    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.containerLaunched();
    wc.verifyState(AMContainerState.IDLE);

    wc.containerCompleted(false);
    wc.verifyState(AMContainerState.COMPLETED);
    verify(wc.tal).registerRunningContainer(wc.containerID);
    verify(wc.tal).unregisterRunningContainer(wc.containerID);
    verify(wc.chh).register(wc.containerID);
    verify(wc.chh).unregister(wc.containerID);

    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(1);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED);

    assertFalse(wc.amContainer.isInErrorState());

    // Pending pull request. (Ideally, container should be dead at this point
    // and this event should not be generated. Network timeout on NM-RM heartbeat
    // can cause it to be genreated)
    wc.pullTaskToRun();
    wc.verifyNoOutgoingEvents();

    assertFalse(wc.amContainer.isInErrorState());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testContainerCompletedAtRunning() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();

    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.containerLaunched();
    wc.pullTaskToRun();
    wc.verifyState(AMContainerState.RUNNING);

    wc.containerCompleted(false);
    wc.verifyState(AMContainerState.COMPLETED);
    verify(wc.tal).registerRunningContainer(wc.containerID);
    verify(wc.tal).unregisterRunningContainer(wc.containerID);
    verify(wc.chh).register(wc.containerID);
    verify(wc.chh).unregister(wc.containerID);

    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(1);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED);

    assertFalse(wc.amContainer.isInErrorState());

    // Pending task complete. (Ideally, container should be dead at this point
    // and this event should not be generated. Network timeout on NM-RM heartbeat
    // can cause it to be genreated)
    wc.taskAttemptSucceeded(wc.taskAttemptID);
    wc.verifyNoOutgoingEvents();

    assertFalse(wc.amContainer.isInErrorState());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testContainerPreemptedAtRunning() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();

    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.containerLaunched();
    wc.pullTaskToRun();
    wc.verifyState(AMContainerState.RUNNING);

    wc.containerCompleted(true);
    wc.verifyState(AMContainerState.COMPLETED);
    verify(wc.tal).registerRunningContainer(wc.containerID);
    verify(wc.tal).unregisterRunningContainer(wc.containerID);
    verify(wc.chh).register(wc.containerID);
    verify(wc.chh).unregister(wc.containerID);

    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(1);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_PREEMPTED);

    assertFalse(wc.amContainer.isInErrorState());

    // Pending task complete. (Ideally, container should be dead at this point
    // and this event should not be generated. Network timeout on NM-RM heartbeat
    // can cause it to be genreated)
    wc.taskAttemptSucceeded(wc.taskAttemptID);
    wc.verifyNoOutgoingEvents();

    assertFalse(wc.amContainer.isInErrorState());
  }
  
  @SuppressWarnings("rawtypes")
  @Test
  public void testTaskAssignedToCompletedContainer() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();
    wc.containerLaunched();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.pullTaskToRun();
    wc.taskAttemptSucceeded(wc.taskAttemptID);

    wc.containerCompleted(false);
    wc.verifyState(AMContainerState.COMPLETED);

    TezTaskAttemptID taID2 = TezTaskAttemptID.getInstance(wc.taskID, 2);

    wc.assignTaskAttempt(taID2);

    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(1);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED);
    TaskAttemptEventContainerTerminated ctEvent =
        (TaskAttemptEventContainerTerminated) outgoingEvents.get(0);
    assertEquals(taID2, ctEvent.getTaskAttemptID());

    // Allocation to a completed Container is considered an error.
    // TODO Is this valid ?
    assertTrue(wc.amContainer.isInErrorState());
  }

  @Test
  public void testTaskPullAtLaunching() {
    WrappedContainer wc = new WrappedContainer();

    wc.launchContainer();
    AMContainerTask pulledTask = wc.pullTaskToRun();
    wc.verifyState(AMContainerState.LAUNCHING);
    wc.verifyNoOutgoingEvents();
    assertFalse(pulledTask.shouldDie());
    assertNull(pulledTask.getTask());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testNodeFailedAtIdle() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();
    wc.containerLaunched();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.verifyState(AMContainerState.IDLE);

    wc.nodeFailed();
    // Expecting a complete event from the RM
    wc.verifyState(AMContainerState.STOPPING);
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(3);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_NODE_FAILED,
        TaskAttemptEventType.TA_CONTAINER_TERMINATING,
        AMSchedulerEventType.S_CONTAINER_DEALLOCATE);

    for (Event event : outgoingEvents) {
      if (event.getType() == TaskAttemptEventType.TA_NODE_FAILED) {
        TaskAttemptEventNodeFailed nfEvent = (TaskAttemptEventNodeFailed) event;
        assertEquals("nodeFailed", nfEvent.getDiagnosticInfo());
      }
    }

    wc.containerCompleted(false);
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(1);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED);

    assertFalse(wc.amContainer.isInErrorState());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testNodeFailedAtIdleMultipleAttempts() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();
    wc.containerLaunched();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.pullTaskToRun();
    wc.taskAttemptSucceeded(wc.taskAttemptID);
    wc.verifyState(AMContainerState.IDLE);

    TezTaskAttemptID taID2 = TezTaskAttemptID.getInstance(wc.taskID, 2);
    wc.assignTaskAttempt(taID2);
    wc.pullTaskToRun();
    wc.taskAttemptSucceeded(taID2);
    wc.verifyState(AMContainerState.IDLE);

    wc.nodeFailed();
    // Expecting a complete event from the RM
    wc.verifyState(AMContainerState.STOPPING);
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(3);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_NODE_FAILED,
        TaskAttemptEventType.TA_NODE_FAILED,
        AMSchedulerEventType.S_CONTAINER_DEALLOCATE);

    for (Event event : outgoingEvents) {
      if (event.getType() == TaskAttemptEventType.TA_NODE_FAILED) {
        TaskAttemptEventNodeFailed nfEvent = (TaskAttemptEventNodeFailed) event;
        assertEquals("nodeFailed", nfEvent.getDiagnosticInfo());
      }
    }

    assertFalse(wc.amContainer.isInErrorState());

    wc.containerCompleted(false);
    wc.verifyNoOutgoingEvents();

    assertNull(wc.amContainer.getRunningTaskAttempt());
    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());
    assertEquals(2, wc.amContainer.getAllTaskAttempts().size());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testNodeFailedAtRunningMultipleAttempts() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();
    wc.containerLaunched();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.pullTaskToRun();
    wc.taskAttemptSucceeded(wc.taskAttemptID);

    TezTaskAttemptID taID2 = TezTaskAttemptID.getInstance(wc.taskID, 2);
    wc.assignTaskAttempt(taID2);
    wc.pullTaskToRun();
    wc.verifyState(AMContainerState.RUNNING);

    wc.nodeFailed();
    // Expecting a complete event from the RM
    wc.verifyState(AMContainerState.STOPPING);
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(4);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_NODE_FAILED,
        TaskAttemptEventType.TA_NODE_FAILED,
        TaskAttemptEventType.TA_CONTAINER_TERMINATING,
        AMSchedulerEventType.S_CONTAINER_DEALLOCATE);

    for (Event event : outgoingEvents) {
      if (event.getType() == TaskAttemptEventType.TA_NODE_FAILED) {
        TaskAttemptEventNodeFailed nfEvent = (TaskAttemptEventNodeFailed) event;
        assertEquals("nodeFailed", nfEvent.getDiagnosticInfo());
      }
    }

    wc.containerCompleted(false);
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(1);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_CONTAINER_TERMINATED);

    assertFalse(wc.amContainer.isInErrorState());
    assertNull(wc.amContainer.getRunningTaskAttempt());
    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());
    assertEquals(2, wc.amContainer.getAllTaskAttempts().size());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testNodeFailedAtCompletedMultipleSuccessfulTAs() {
    WrappedContainer wc = new WrappedContainer();
    List<Event> outgoingEvents;

    wc.launchContainer();
    wc.containerLaunched();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.pullTaskToRun();
    wc.taskAttemptSucceeded(wc.taskAttemptID);

    TezTaskAttemptID taID2 = TezTaskAttemptID.getInstance(wc.taskID, 2);
    wc.assignTaskAttempt(taID2);
    wc.pullTaskToRun();
    wc.taskAttemptSucceeded(taID2);
    wc.stopRequest();
    wc.nmStopSent();
    wc.containerCompleted(false);
    wc.verifyState(AMContainerState.COMPLETED);

    wc.nodeFailed();
    outgoingEvents = wc.verifyCountAndGetOutgoingEvents(2);
    verifyUnOrderedOutgoingEventTypes(outgoingEvents,
        TaskAttemptEventType.TA_NODE_FAILED,
        TaskAttemptEventType.TA_NODE_FAILED);

    assertNull(wc.amContainer.getRunningTaskAttempt());
    assertEquals(0, wc.amContainer.getQueuedTaskAttempts().size());
    assertEquals(2, wc.amContainer.getAllTaskAttempts().size());
  }

  @Test
  public void testDuplicateCompletedEvents() {
    WrappedContainer wc = new WrappedContainer();

    wc.launchContainer();
    wc.containerLaunched();
    wc.assignTaskAttempt(wc.taskAttemptID);
    wc.pullTaskToRun();
    wc.taskAttemptSucceeded(wc.taskAttemptID);

    TezTaskAttemptID taID2 = TezTaskAttemptID.getInstance(wc.taskID, 2);
    wc.assignTaskAttempt(taID2);
    wc.pullTaskToRun();
    wc.taskAttemptSucceeded(taID2);
    wc.stopRequest();
    wc.nmStopSent();
    wc.containerCompleted(false);
    wc.verifyState(AMContainerState.COMPLETED);

    wc.verifyNoOutgoingEvents();

    wc.containerCompleted(false);
    wc.verifyNoOutgoingEvents();
  }
  
  @Test
  public void testLocalResourceAddition() {
    WrappedContainer wc = new WrappedContainer();

    String rsrc1 = "rsrc1";
    String rsrc2 = "rsrc2";
    String rsrc3 = "rsrc3";

    Map<String, LocalResource> initialResources = Maps.newHashMap();
    initialResources.put(rsrc1, createLocalResource(rsrc1));

    wc.launchContainer(initialResources, new Credentials());
    wc.containerLaunched();
    wc.assignTaskAttempt(wc.taskAttemptID);
    AMContainerTask task1 = wc.pullTaskToRun();
    assertEquals(0, task1.getAdditionalResources().size());
    wc.taskAttemptSucceeded(wc.taskAttemptID);

    // Add some resources to the next task.
    Map<String, LocalResource> additionalResources = Maps.newHashMap();
    additionalResources.put(rsrc2, createLocalResource(rsrc2));
    additionalResources.put(rsrc3, createLocalResource(rsrc3));

    TezTaskAttemptID taID2 = TezTaskAttemptID.getInstance(wc.taskID, 2);
    wc.assignTaskAttempt(taID2, additionalResources, new Credentials());
    AMContainerTask task2 = wc.pullTaskToRun();
    Map<String, LocalResource> pullTaskAdditionalResources = task2.getAdditionalResources();
    assertEquals(2, pullTaskAdditionalResources.size());
    pullTaskAdditionalResources.remove(rsrc2);
    pullTaskAdditionalResources.remove(rsrc3);
    assertEquals(0, pullTaskAdditionalResources.size());
    wc.taskAttemptSucceeded(taID2);

    // Verify Resources registered for this container.
    Map<String, LocalResource> containerLRs = new HashMap<String, LocalResource>(
        wc.amContainer.containerLocalResources);
    assertEquals(3, containerLRs.size());
    containerLRs.remove(rsrc1);
    containerLRs.remove(rsrc2);
    containerLRs.remove(rsrc3);
    assertEquals(0, containerLRs.size());

    // Try launching another task with the same reosurces as Task2. Verify the
    // task is not asked to re-localize again.
    TezTaskAttemptID taID3 = TezTaskAttemptID.getInstance(wc.taskID, 3);
    wc.assignTaskAttempt(taID3, new HashMap<String, LocalResource>(), new Credentials());
    AMContainerTask task3 = wc.pullTaskToRun();
    assertEquals(0, task3.getAdditionalResources().size());
    wc.taskAttemptSucceeded(taID3);

    // Verify references are cleared after a container completes.
    wc.containerCompleted(false);
    assertNull(wc.amContainer.containerLocalResources);
    assertNull(wc.amContainer.additionalLocalResources);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testCredentialsTransfer() {
    WrappedContainerMultipleDAGs wc = new WrappedContainerMultipleDAGs();

    TezDAGID dagID2 = TezDAGID.getInstance("800", 500, 2);
    TezDAGID dagID3 = TezDAGID.getInstance("800", 500, 3);
    TezVertexID vertexID2 = TezVertexID.getInstance(dagID2, 1);
    TezVertexID vertexID3 = TezVertexID.getInstance(dagID3, 1);
    TezTaskID taskID2 = TezTaskID.getInstance(vertexID2, 1);
    TezTaskID taskID3 = TezTaskID.getInstance(vertexID3, 1);

    TezTaskAttemptID attempt11 = TezTaskAttemptID.getInstance(wc.taskID, 200);
    TezTaskAttemptID attempt12 = TezTaskAttemptID.getInstance(wc.taskID, 300);
    TezTaskAttemptID attempt21 = TezTaskAttemptID.getInstance(taskID2, 200);
    TezTaskAttemptID attempt22 = TezTaskAttemptID.getInstance(taskID2, 300);
    TezTaskAttemptID attempt31 = TezTaskAttemptID.getInstance(taskID3, 200);
    TezTaskAttemptID attempt32 = TezTaskAttemptID.getInstance(taskID3, 300);
    
    Map<String, LocalResource> LRs = new HashMap<String, LocalResource>();
    AMContainerTask fetchedTask = null;
    
    Token<TokenIdentifier> amGenToken = mock(Token.class);
    Token<TokenIdentifier> token1 = mock(Token.class);
    Token<TokenIdentifier> token3 = mock(Token.class);
    
    Credentials containerCredentials = new Credentials();
    TokenCache.setSessionToken(amGenToken, containerCredentials);

    Text token1Name = new Text("tokenDag1");
    Text token3Name = new Text("tokenDag3");
    
    Credentials dag1Credentials = new Credentials();
    dag1Credentials.addToken(new Text(token1Name), token1);
    Credentials dag3Credentials = new Credentials();
    dag3Credentials.addToken(new Text(token3Name), token3);
    
    wc.launchContainer(new HashMap<String, LocalResource>(), containerCredentials);
    wc.containerLaunched();
    wc.assignTaskAttempt(attempt11, LRs , dag1Credentials);
    fetchedTask = wc.pullTaskToRun();
    assertTrue(fetchedTask.haveCredentialsChanged());
    assertNotNull(fetchedTask.getCredentials());
    assertNotNull(fetchedTask.getCredentials().getToken(token1Name));
    wc.taskAttemptSucceeded(attempt11);
    
    wc.assignTaskAttempt(attempt12, LRs, dag1Credentials);
    fetchedTask = wc.pullTaskToRun();
    assertFalse(fetchedTask.haveCredentialsChanged());
    assertNull(fetchedTask.getCredentials());
    wc.taskAttemptSucceeded(attempt12);
    
    // Move to running a second DAG, with no credentials.
    wc.setNewDAGID(dagID2);
    wc.assignTaskAttempt(attempt21, LRs, null);
    fetchedTask = wc.pullTaskToRun();
    assertTrue(fetchedTask.haveCredentialsChanged());
    assertNull(fetchedTask.getCredentials());
    wc.taskAttemptSucceeded(attempt21);
    
    wc.assignTaskAttempt(attempt22, LRs, null);
    fetchedTask = wc.pullTaskToRun();
    assertFalse(fetchedTask.haveCredentialsChanged());
    assertNull(fetchedTask.getCredentials());
    wc.taskAttemptSucceeded(attempt22);
    
    // Move to running a third DAG, with Credentials this time
    wc.setNewDAGID(dagID3);
    wc.assignTaskAttempt(attempt31, LRs , dag3Credentials);
    fetchedTask = wc.pullTaskToRun();
    assertTrue(fetchedTask.haveCredentialsChanged());
    assertNotNull(fetchedTask.getCredentials());
    assertNotNull(fetchedTask.getCredentials().getToken(token3Name));
    assertNull(fetchedTask.getCredentials().getToken(token1Name));
    wc.taskAttemptSucceeded(attempt31);
    
    wc.assignTaskAttempt(attempt32, LRs, dag1Credentials);
    fetchedTask = wc.pullTaskToRun();
    assertFalse(fetchedTask.haveCredentialsChanged());
    assertNull(fetchedTask.getCredentials());
    wc.taskAttemptSucceeded(attempt32);
  }
  
  @SuppressWarnings("rawtypes")
  @Test
  public void testContainerProfiling() {
    WrappedContainer wc = new WrappedContainer(true, "profileString");
    wc.launchContainer();
    List<Event> events = wc.verifyCountAndGetOutgoingEvents(1);
    Event event = events.get(0);
    assertTrue(event instanceof NMCommunicatorLaunchRequestEvent);
    NMCommunicatorLaunchRequestEvent lrEvent = (NMCommunicatorLaunchRequestEvent) event;
    ContainerLaunchContext clc = lrEvent.getContainerLaunchContext();
    assertNotNull(clc);
    assertTrue(clc.getCommands().get(0).contains("profileString"));
  }

  // TODO Verify diagnostics in most of the tests.

  private static class WrappedContainer {

    long rmIdentifier = 2000;
    ApplicationId applicationID;
    ApplicationAttemptId appAttemptID;
    ContainerId containerID;
    NodeId nodeID;
    String nodeHttpAddress;
    Resource resource;
    Priority priority;
    Container container;
    ContainerHeartbeatHandler chh;
    TaskAttemptListener tal;

    @SuppressWarnings("rawtypes")
    EventHandler eventHandler;

    AppContext appContext;

    TezDAGID dagID;
    TezVertexID vertexID;
    TezTaskID taskID;
    TezTaskAttemptID taskAttemptID;

    TaskSpec taskSpec;

    public AMContainerImpl amContainer;

    public WrappedContainer(boolean shouldProfile, String profileString) {
      applicationID = ApplicationId.newInstance(rmIdentifier, 1);
      appAttemptID = ApplicationAttemptId.newInstance(applicationID, 1);
      containerID = ContainerId.newInstance(appAttemptID, 1);
      nodeID = NodeId.newInstance("host", 12500);
      nodeHttpAddress = "host:12501";
      resource = Resource.newInstance(1024, 1);
      priority = Priority.newInstance(1);
      container = Container.newInstance(containerID, nodeID,
          nodeHttpAddress, resource, priority, null);

      chh = mock(ContainerHeartbeatHandler.class);

      InetSocketAddress addr = new InetSocketAddress("localhost", 0);
      tal = mock(TaskAttemptListener.class);
      doReturn(addr).when(tal).getAddress();

      dagID = TezDAGID.getInstance(applicationID, 1);
      vertexID = TezVertexID.getInstance(dagID, 1);
      taskID = TezTaskID.getInstance(vertexID, 1);
      taskAttemptID = TezTaskAttemptID.getInstance(taskID, 1);
      
      eventHandler = mock(EventHandler.class);

      appContext = mock(AppContext.class);
      doReturn(new HashMap<ApplicationAccessType, String>()).when(appContext)
      .getApplicationACLs();
      doReturn(eventHandler).when(appContext).getEventHandler();
      doReturn(appAttemptID).when(appContext).getApplicationAttemptId();
      doReturn(applicationID).when(appContext).getApplicationID();
      mockDAGID();

      taskSpec = mock(TaskSpec.class);
      doReturn(taskAttemptID).when(taskSpec).getTaskAttemptID();

      amContainer = new AMContainerImpl(container, chh, tal,
          new ContainerContextMatcher(), shouldProfile, profileString, appContext);
    }
    
    public WrappedContainer() {
      this(false, null);
    }
    
    protected void mockDAGID() {
      doReturn(dagID).when(appContext).getCurrentDAGID();
    }

    /**
     * Verifies no additional outgoing events generated by the last incoming
     * event to the AMContainer.
     */
    @SuppressWarnings("unchecked")
    public void verifyNoOutgoingEvents() {
      verify(eventHandler, never()).handle(any(Event.class));
    }

    /**
     * Returns a list of outgoing events generated by the last incoming event to
     * the AMContainer.
     * @param invocations number of expected invocations.
     *
     * @return a list of outgoing events from the AMContainer.
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List<Event> verifyCountAndGetOutgoingEvents(int invocations) {
      ArgumentCaptor<Event> args = ArgumentCaptor.forClass(Event.class);
      verify(eventHandler, times(invocations)).handle(args.capture());
      return args.getAllValues();
    }

    public void launchContainer() {
      launchContainer(new HashMap<String, LocalResource>(), new Credentials());
    }

    public void launchContainer(Map<String, LocalResource> localResources, Credentials credentials) {
      reset(eventHandler);
      @SuppressWarnings("unchecked")
      Token<JobTokenIdentifier> jobToken = mock(Token.class);
      TokenCache.setSessionToken(jobToken, credentials);
      amContainer.handle(new AMContainerEventLaunchRequest(containerID, vertexID,
          new ContainerContext(localResources, credentials, new HashMap<String, String>(), "")));
    }

    public void assignTaskAttempt(TezTaskAttemptID taID) {
      assignTaskAttempt(taID, new HashMap<String, LocalResource>(), new Credentials());
    }

    public void assignTaskAttempt(TezTaskAttemptID taID,
        Map<String, LocalResource> additionalResources, Credentials credentials) {
      reset(eventHandler);
      amContainer.handle(new AMContainerEventAssignTA(containerID, taID, taskSpec,
          additionalResources, credentials));
    }

    public AMContainerTask pullTaskToRun() {
      reset(eventHandler);
      return amContainer.pullTaskContext();
    }

    public void containerLaunched() {
      reset(eventHandler);
      amContainer.handle(new AMContainerEventLaunched(containerID));
    }

    public void taskAttemptSucceeded(TezTaskAttemptID taID) {
      reset(eventHandler);
      amContainer.handle(new AMContainerEventTASucceeded(containerID, taID));
    }

    public void stopRequest() {
      reset(eventHandler);
      amContainer.handle(new AMContainerEventStopRequest(containerID));
    }

    public void nmStopSent() {
      reset(eventHandler);
      amContainer.handle(new AMContainerEvent(containerID,
          AMContainerEventType.C_NM_STOP_SENT));
    }

    public void nmStopFailed() {
      reset(eventHandler);
      amContainer.handle(new AMContainerEvent(containerID,
          AMContainerEventType.C_NM_STOP_FAILED));
    }

    public void containerCompleted(boolean preempted) {
      reset(eventHandler);
      ContainerStatus cStatus = ContainerStatus.newInstance(containerID,
          ContainerState.COMPLETE, "", 100);
      amContainer.handle(new AMContainerEventCompleted(cStatus, preempted));
    }

    public void containerTimedOut() {
      reset(eventHandler);
      amContainer.handle(new AMContainerEvent(containerID,
          AMContainerEventType.C_TIMED_OUT));
    }

    public void launchFailed() {
      reset(eventHandler);
      amContainer.handle(new AMContainerEventLaunchFailed(containerID,
          "launchFailed"));
    }

    public void nodeFailed() {
      reset(eventHandler);
      amContainer.handle(new AMContainerEventNodeFailed(containerID,
          "nodeFailed"));
    }

    public void verifyState(AMContainerState state) {
      assertEquals(
          "Expected state: " + state + ", but found: " + amContainer.getState(),
          state, amContainer.getState());
    }
  }
  
  private static class WrappedContainerMultipleDAGs extends WrappedContainer {
    
    private TezDAGID newDAGID = null;
    
    @Override
    protected void mockDAGID() {
      doAnswer(new Answer<TezDAGID>() {
        @Override
        public TezDAGID answer(InvocationOnMock invocation) throws Throwable {
          return newDAGID == null ? dagID : newDAGID;
        }
      }).when(appContext).getCurrentDAGID();
    }
    
    void setNewDAGID(TezDAGID newDAGID) {
      this.newDAGID = newDAGID;
    }
  }

  @SuppressWarnings("rawtypes")
  private void verifyUnOrderedOutgoingEventTypes(List<Event> events,
      Enum<?>... expectedTypes) {

    List<Enum<?>> expectedTypeList = new LinkedList<Enum<?>>();
    for (Enum<?> expectedType : expectedTypes) {
      expectedTypeList.add(expectedType);
    }
    List<Event> eventsCopy = new LinkedList<Event>(events);

    Iterator<Enum<?>> expectedTypeIterator = expectedTypeList.iterator();
    while (expectedTypeIterator.hasNext()) {
      Enum<?> expectedType = expectedTypeIterator.next();
      Iterator<Event> iter = eventsCopy.iterator();
      while (iter.hasNext()) {
        Event e = iter.next();
        if (e.getType() == expectedType) {
          iter.remove();
          expectedTypeIterator.remove();
          break;
        }
      }
    }
    assertTrue("Did not find types : " + expectedTypeList
        + " in outgoing event list", expectedTypeList.isEmpty());
    assertTrue("Found unexpected events: " + eventsCopy
        + " in outgoing event list", eventsCopy.isEmpty());
  }
  
  private LocalResource createLocalResource(String name) {
    LocalResource lr = LocalResource.newInstance(URL.newInstance(null, "localhost", 2321, name),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, 1, 1000000);
    return lr;
  }
}
