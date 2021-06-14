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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.ssh.SSHKeyPair;
import io.cdap.cdap.runtime.spi.ssh.SSHPublicKey;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Configuration for EMR.
 */
public class EMRConf {
  private final String accessKey;
  private final String secretKey;
  private final String region;

  private EMRConf(String accessKey, String secretKey, String region) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.region = region;
  }

  String getRegion() {
    return region;
  }

  AWSCredentialsProvider getCredentialsProvider() {
    return new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey));
  }

  static EMRConf fromProvisionerContext(ProvisionerContext context) {
    Optional<SSHKeyPair> sshKeyPair = context.getSSHContext().getSSHKeyPair();
    return create(context.getProperties(), sshKeyPair.map(SSHKeyPair::getPublicKey).orElse(null));
  }

  /**
   * Create the conf from a property map while also performing validation.
   */
  static EMRConf fromProperties(Map<String, String> properties) {
    return create(properties, null);
  }

  private static EMRConf create(Map<String, String> properties, @Nullable SSHPublicKey publicKey) {
    String accessKey = getString(properties, "accessKey");
    String secretKey = getString(properties, "secretKey");

    String region = getString(properties, "region", Regions.DEFAULT_REGION.getName());

    return new EMRConf(accessKey, secretKey, region);
  }

  private static String getString(Map<String, String> properties, String key) {
    String val = properties.get(key);
    if (val == null) {
      throw new IllegalArgumentException(String.format("Invalid config. '%s' must be specified.", key));
    }
    return val;
  }

  private static String getString(Map<String, String> properties, String key, String defaultVal) {
    String val = properties.get(key);
    return val == null ? defaultVal : val;
  }
}
