/*
 * Copyright © 2018 Cask Data, Inc.
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

import com.amazonaws.services.emrcontainers.AmazonEMRContainersClientBuilder;
import com.amazonaws.services.emrcontainers.model.*;
import com.amazonaws.services.emrcontainers.model.DescribeVirtualClusterResult;
import com.amazonaws.services.emrcontainers.AmazonEMRContainers;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.runtime.spi.provisioner.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.ARRESTED;

/**
 * Wrapper around the EMR client that adheres to our configuration settings.
 */
public class EMRContainersClient implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(EMRContainersClient.class);

  private static final Set UNTERMINATED_STATES = ImmutableSet.of(ClusterStatus.RUNNING, VirtualClusterState.ARRESTED);

  private final EMRConf emrConf;
  private final AmazonEMRContainers client;

  static EMRContainersClient fromConf(EMRConf conf) {
    AmazonEMRContainersClientBuilder standard = AmazonEMRContainersClientBuilder.standard();
    standard.setCredentials(conf.getCredentialsProvider());
    standard.withRegion(conf.getRegion());
    AmazonEMRContainers client = standard.build();
    return new EMRContainersClient(conf, client);
  }

  private EMRContainersClient(EMRConf emrConf, AmazonEMRContainers client) {
    this.emrConf = emrConf;
    this.client = client;
  }
  /**
   * Create a virtual cluster. This will return after the initial request to create the cluster is completed.
   *
   * @param name the name of the cluster to create
   * @return the id of the created EMR cluster
   */
  public String createVirtualCluster(String name) {

    ContainerInfo containerInfo = new ContainerInfo()
      .withEksInfo(new EksInfo().withNamespace("cdap"));

    ContainerProvider containerProvider = new ContainerProvider()
      .withId("wp-cdap")
      .withType("EKS")
      .withInfo(containerInfo);

    Map<String,String> clusterTags = new HashMap<String,String>();
    clusterTags.put("Application","CDAP");

    CreateVirtualClusterRequest request = new CreateVirtualClusterRequest()
      .withName(name)
      .withContainerProvider(containerProvider)
      .withTags(clusterTags);

    LOG.info("Creating virtual cluster {}.", name);
    CreateVirtualClusterResult result = client.createVirtualCluster(request);

    LOG.debug(result.toString());

    if (result.getId().isEmpty()) {
      LOG.error("Error creating cluster {}.", name);
      return null;
    } else {
      LOG.info("Created cluster: {}, id: {}, arn: {}.", name, result.getId(), result.getArn());
      return result.getId();
    }

  }

  /**
   * Delete the specified cluster if it exists. This will return after the initial request to delete the cluster
   * is completed. At this point, the cluster is likely not yet deleted, but in a deleting state.
   *
   * @param id the id of the cluster to delete
   */
  public void deleteVirtualCluster(String id) {
    LOG.info("Deleting virtual cluster {}.", id);

    client.deleteVirtualCluster(new DeleteVirtualClusterRequest().withId(id));
  }

  /**
   * Get information about the specified cluster. The cluster will not be present if it could not be found.
   *
   * @param id the cluster id
   * @return the cluster information if it exists
   */
  public boolean existsVirtualCluster(String id) {
    try {
      DescribeVirtualClusterResult cluster = client.describeVirtualCluster(new DescribeVirtualClusterRequest().withId(id));
    } catch (Exception ResourceNotFoundException) {
      return false;
    }
    return true;
  }

  /**
   * Get the status of the specified virtual cluster.
   *
   * @param id the cluster id
   * @return the cluster status
   */
  public io.cdap.cdap.runtime.spi.provisioner.VirtualClusterStatus getVirtualClusterStatus(String id) {
    return convertStatus(describeVirtualCluster(id).getState());
  }
  /**
   * Get information about the specified cluster. The cluster will not be present if it could not be found.
   *
   * @param id the cluster id
   * @return the cluster information if it exists
   */
  public Optional<io.cdap.cdap.runtime.spi.provisioner.Cluster> getCluster(String id) {
    VirtualCluster cluster = describeVirtualCluster(id);

    List<Node> nodes = new ArrayList<>();

    return Optional.of(new io.cdap.cdap.runtime.spi.provisioner.Cluster(
            cluster.getId(), convertStatus(cluster.getState()), nodes, Collections.emptyMap()));
  }
  private VirtualCluster describeVirtualCluster(String id) {
    return client.describeVirtualCluster(new DescribeVirtualClusterRequest().withId(id)).getVirtualCluster();
  }

  private io.cdap.cdap.runtime.spi.provisioner.ClusterStatus convertStatus(VirtualClusterState status) {
    switch (status) {
      case ARRESTED:
      case RUNNING:
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.RUNNING;
      case TERMINATING:
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.TERMINATING;
      case TERMINATED:
        // we don't returned FAILED, because then that means we will attempt to delete it
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.NOT_EXISTS;
      default:
        // unrecognized and unknown
        return io.cdap.cdap.runtime.spi.provisioner.ClusterStatus.ORPHANED;
    }
  }
  @Override
  public void close() {
    client.shutdown();
  }
}
