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

package org.apache.tez.dag.api;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

/**
 * This interface defines the routing of the event between tasks of producer and 
 * consumer vertices. The routing is bi-directional. Users can customize the 
 * routing by providing an implementation of this interface.
 * 
 * Implementations must provide a 0 argument public constructor.
 */
@Evolving
public interface EdgeManager {
  
  /**
   * Initializes the EdgeManager. This method is called in the following
   * circumstances </p> 1. when initializing an Edge Manager for the first time.
   * </p> 2. When an EdgeManager is replaced at runtime. At this point, an
   * EdgeManager instance is created and setup by the user. The initialize
   * method will be called with the original {@link EdgeManagerContext} when the
   * edgeManager is replaced.
   * 
   * @param edgeManagerContext
   *          the context within which this EdgeManager will run. Includes
   *          information like configuration which the user may have specified
   *          while setting up the edge.
   */
  public void initialize(EdgeManagerContext edgeManagerContext);
  
  /**
   * Get the number of physical inputs on the destination task
   * @param numSourceTasks Total number of source tasks
   * @param destinationTaskIndex Index of destination task for which number of 
   * inputs is needed
   * @return Number of physical inputs on the destination task
   */
  public int getNumDestinationTaskPhysicalInputs(int numSourceTasks, 
      int destinationTaskIndex);

  /**
   * Get the number of physical outputs on the source task
   * @param numDestinationTasks Total number of destination tasks
   * @param sourceTaskIndex Index of the source task for which number of outputs 
   * is needed
   * @return Number of physical outputs on the source task
   */
  public int getNumSourceTaskPhysicalOutputs(int numDestinationTasks, 
      int sourceTaskIndex);
  
  /**
   * Return the routing information to inform consumers about the source task
   * output that is now available. The return Map has the routing information.
   * Key is the destination task physical input index and the value is the list
   * of destination task indices for which the key input index will receive the
   * data movement event.
   * @param event
   *          Data movement event
   * @param sourceTaskIndex
   *          Source task
   * @param numDestinationTasks
   *          Total number of destination tasks
   * @param inputIndicesToTaskIndices
   *          Map via which the routing information is returned
   */
  public void routeDataMovementEventToDestination(DataMovementEvent event,
      int sourceTaskIndex, int numDestinationTasks,
      Map<Integer, List<Integer>> inputIndicesToTaskIndices);
  
  /**
   * Return the routing information to inform consumers about the failure of a
   * source task whose outputs have been potentially lost. The return Map has
   * the routing information. Key is the destination task physical input index
   * and the value is the list of destination task indices for which the key
   * input index will receive the input failure notification. This method will
   * be called once for every source task failure and information for all
   * affected destinations must be provided in that invocation.
   * 
   * @param sourceTaskIndex
   *          Source task
   * @param numDestinationTasks
   *          Total number of destination tasks
   * @param inputIndicesToTaskIndices
   *          Map via which the routing information is returned
   */
  public void routeInputSourceTaskFailedEventToDestination(int sourceTaskIndex,
      int numDestinationTasks,
      Map<Integer, List<Integer>> inputIndicesToTaskIndices);

  /**
   * Get the number of destination tasks that consume data from the source task
   * @param sourceTaskIndex Source task index
   * @param numDestinationTasks Total number of destination tasks
   */
  public int getNumDestinationConsumerTasks(int sourceTaskIndex, int numDestinationTasks);
  
  /**
   * Return the source task index to which to send the input error event
   * @param event Input read error event. Has more information about the error
   * @param destinationTaskIndex Destination task that reported the error
   * @return Index of the source task that created the unavailable input
   */
  public int routeInputErrorEventToSource(InputReadErrorEvent event,
      int destinationTaskIndex);
  
}
