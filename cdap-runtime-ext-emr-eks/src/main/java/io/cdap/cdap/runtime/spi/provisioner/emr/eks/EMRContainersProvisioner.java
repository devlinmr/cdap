/*
 * Copyright © 2018-2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.runtime.spi.provisioner.emr.eks;

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.runtime.spi.ProgramRunInfo;
import io.cdap.cdap.runtime.spi.provisioner.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Provisions a cluster using Amazon EMR.
 */
public class EMRContainersProvisioner implements Provisioner {

  private static final Logger LOG = LoggerFactory.getLogger(EMRContainersProvisioner.class);
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
    "aws-emr-eks", "Amazon EMR on EKS",
    "Amazon EMR on EKS provides a managed EMR service on EKS");
  private static final String CLUSTER_PREFIX = "cdap-";

  @Override
  public ProvisionerSpecification getSpec() {
    return SPEC;
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    EMRConf.fromProperties(properties);
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) throws Exception {

    EMRConf conf = EMRConf.fromProvisionerContext(context);
    String clusterName = getClusterName(context.getProgramRunInfo());

    try (EMRContainersClient client = EMRContainersClient.fromConf(conf)) {
      // if it already exists, it means this is a retry. We can skip actually making the request
      if (client.existsVirtualCluster()) {
        return client.existsVirtualCluster(existing.get().getId()).get();
      }
      String clusterId = client.createVirtualCluster(clusterName);
      // nodes is empty for a Virtual Cluster
      List<Node> nodes = new ArrayList<>();
      return new Cluster(clusterId, ClusterStatus.RUNNING, nodes, Collections.emptyMap());
    }
  }

  @Override
  public VirtualClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster) throws Exception {
    EMRConf conf = EMRConf.fromProvisionerContext(context);
    try (EMRContainersClient client = EMRContainersClient.fromConf(conf)) {
      return client.getVirtualClusterStatus(cluster.getName());
    }
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context,
                                  Cluster cluster) throws Exception {
    EMRConf conf = EMRConf.fromProvisionerContext(context);
    try (EMRContainersClient client = EMRContainersClient.fromConf(conf)) {
      Optional<Cluster> existing = client.getCluster(cluster.getName());
      return existing.orElseGet(() -> new Cluster(cluster, ClusterStatus.NOT_EXISTS));
    }
  }

  @Override
  public void deleteCluster(ProvisionerContext context, Cluster cluster) throws Exception {
    EMRConf conf = EMRConf.fromProvisionerContext(context);
    try (EMRContainersClient client = EMRContainersClient.fromConf(conf)) {
      client.deleteVirtualCluster(cluster.getName());
    }
  }

  @Override
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    return PollingStrategies.fixedInterval(20, TimeUnit.SECONDS);
  }

  // Name must start with a lowercase letter followed by up to 51 lowercase letters,
  // numbers, or hyphens, and cannot end with a hyphen
  // We'll use app-runid, where app is truncated to fit, lowercased, and stripped of invalid characters
  @VisibleForTesting
  static String getClusterName(ProgramRunInfo programRunInfo) {
    String cleanedAppName = programRunInfo.getApplication().replaceAll("[^A-Za-z0-9\\-]", "").toLowerCase();
    // 51 is max length, need to subtract the prefix and 1 extra for the '-' separating app name and run id
    int maxAppLength = 51 - CLUSTER_PREFIX.length() - 1 - programRunInfo.getRun().length();
    if (cleanedAppName.length() > maxAppLength) {
      cleanedAppName = cleanedAppName.substring(0, maxAppLength);
    }
    return CLUSTER_PREFIX + cleanedAppName + "-" + programRunInfo.getRun();
  }

  @Override
  public Capabilities getCapabilities() {
    return new Capabilities(Collections.unmodifiableSet(new HashSet<>(Arrays.asList("fileSet", "externalDataset"))));
  }
}
