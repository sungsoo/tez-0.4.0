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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerHeartbeatHandler;
import org.apache.tez.dag.app.TaskAttemptListener;

public class AMContainerMap extends AbstractService implements EventHandler<AMContainerEvent> {

  private static final Log LOG = LogFactory.getLog(AMContainerMap.class);

  private final ContainerHeartbeatHandler chh;
  private final TaskAttemptListener tal;
  private final AppContext context;
  private final ContainerSignatureMatcher containerSignatureMatcher;
  private final ConcurrentHashMap<ContainerId, AMContainer> containerMap;
  private Set<Integer> profileContainerSet;
  private String commonProfileJavaOpts;

  public AMContainerMap(ContainerHeartbeatHandler chh, TaskAttemptListener tal,
      ContainerSignatureMatcher containerSignatureMatcher, AppContext context) {
    super("AMContainerMaps");
    this.chh = chh;
    this.tal = tal;
    this.context = context;
    this.containerSignatureMatcher = containerSignatureMatcher;
    this.containerMap = new ConcurrentHashMap<ContainerId, AMContainer>();
  }

  @Override
  public synchronized void serviceInit(Configuration conf) {
    Collection<String> profileContainers = getConfig().getTrimmedStringCollection(
        TezConfiguration.TEZ_PROFILE_CONTAINER_LIST);
    if (profileContainers != null && !profileContainers.isEmpty()) {
      profileContainerSet = new HashSet<Integer>();
      for (String containerNum : profileContainers) {
        profileContainerSet.add(Integer.parseInt(containerNum));
      }
      commonProfileJavaOpts = conf.get(TezConfiguration.TEZ_PROFILE_JVM_OPTS);
      if (!profileContainerSet.isEmpty()
          && (commonProfileJavaOpts == null || commonProfileJavaOpts.isEmpty())) {
        LOG.warn("Profiling specified for " + profileContainerSet.size()
            + " containers, but no profiling string specified. "
            + TezConfiguration.TEZ_PROFILE_JVM_OPTS + " needs to be set");
      }
      LOG.info("Containers to be profile: " + profileContainerSet );
    }
    
  }

  @Override
  public void handle(AMContainerEvent event) {
    AMContainer container = containerMap.get(event.getContainerId());
    if (container != null) {
      container.handle(event);
    } else {
      LOG.info("Event for unknown container: " + event.getContainerId());
    }
  }

  public boolean addContainerIfNew(Container container) {
    boolean shouldProfile = profileContainerSet == null ? false : profileContainerSet
        .contains(container.getId().getId());
    AMContainer amc = new AMContainerImpl(container, chh, tal, containerSignatureMatcher,
        shouldProfile, shouldProfile ? commonProfileJavaOpts : null, context);
    return (containerMap.putIfAbsent(container.getId(), amc) == null);
  }

  public AMContainer get(ContainerId containerId) {
    return containerMap.get(containerId);
  }

  public Collection<AMContainer> values() {
    return containerMap.values();
  }
}
