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

package org.apache.tez.runtime.api.events;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.runtime.api.Event;

/**
 * Event sent from the AM to an Input to indicate that one of it's sources has
 * failed - effectively the input is no longer available from the particular
 * source.
 * Users are not expected to send this event.
 */
public class InputFailedEvent extends Event{

  /**
   * Index(i) of the i-th (physical) Input or Output that generated the data.
   * For a Processor-generated event, this is ignored.
   */
  private int sourceIndex;

  /**
   * Index(i) of the i-th (physical) Input or Output that is meant to receive
   * this Event. For a Processor event, this is ignored.
   */
  private int targetIndex;

  /**
   * Version number to indicate what attempt generated this Event
   */
  private int version;
  
  @Private // for Writable
  public InputFailedEvent() {
  }
  
  @Private
  public InputFailedEvent(int sourceIndex,
      int targetIndex,
      int version) {
    this.sourceIndex = sourceIndex;
    this.targetIndex = targetIndex;
    this.version = version;
  }
  
  @Private
  public InputFailedEvent(int targetIndex, int version) {
    this.targetIndex = targetIndex;
    this.version = version;
  }

  public int getSourceIndex() {
    return sourceIndex;
  }

  public int getTargetIndex() {
    return targetIndex;
  }

  @Private
  public void setTargetIndex(int targetIndex) {
    this.targetIndex = targetIndex;
  }

  public int getVersion() {
    return version;
  }

  @Private
  public void setVersion(int version) {
    this.version = version;
  }
}
