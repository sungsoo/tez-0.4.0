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

import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.runtime.api.impl.TaskSpec;

public class AMContainerTask {
  private final boolean shouldDie;
  private final Map<String, LocalResource> additionalResources;
  private final TaskSpec tezTask;
  private final Credentials credentials;
  private final boolean credentialsChanged;

  public AMContainerTask(boolean shouldDie, TaskSpec tezTask,
      Map<String, LocalResource> additionalResources, Credentials credentials, boolean credentialsChanged) {
    this.shouldDie = shouldDie;
    this.tezTask = tezTask;
    this.additionalResources = additionalResources;
    this.credentials = credentials;
    this.credentialsChanged = credentialsChanged;
  }

  public boolean shouldDie() {
    return this.shouldDie;
  }

  public TaskSpec getTask() {
    return this.tezTask;
  }

  public Map<String, LocalResource> getAdditionalResources() {
    return this.additionalResources;
  }
  
  public Credentials getCredentials() {
    return this.credentials;
  }
  
  public boolean haveCredentialsChanged() {
    return this.credentialsChanged;
  }
}
