/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.worker.sidecar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.CharStreams;
import com.google.inject.Inject;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.retry.RetryableException;
import io.cdap.cdap.common.ArtifactNotFoundException;
import io.cdap.cdap.common.ServiceUnavailableException;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.lang.jar.BundleJarUtil;
import io.cdap.cdap.common.service.Retries;
import io.cdap.cdap.common.service.RetryStrategies;
import io.cdap.cdap.common.service.RetryStrategy;
import io.cdap.cdap.proto.id.ArtifactId;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequestConfig;
import org.apache.commons.io.FileUtils;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.resteasy.util.HttpHeaderNames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

/**
 * ArtifactLocalizer is responsible for fetching, caching and unpacking artifacts requested by the worker pod. The HTTP
 * endpoints are defined in {@link ArtifactLocalizerHttpHandlerInternal}. This class will run in the sidecar container
 * that is defined by {@link ArtifactLocalizerTwillRunnable}.
 * <p>
 * Artifacts will be cached using the following file structure: /PD_DIRECTORY/artifacts/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>.jar
 * Artifacts will be unpacked using the following file structure: /PD_DIRECTORY/unpacked/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>/...
 */
public class ArtifactLocalizer {

  private static final Logger LOG = LoggerFactory.getLogger(ArtifactLocalizer.class);
  private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.RFC_1123_DATE_TIME;

  // TODO: Move this directory name into a cConf property
  private static final String PD_DIR = "data/";
  public static final String LAST_MODIFIED_HEADER = HttpHeaderNames.LAST_MODIFIED.toLowerCase();
  private final RemoteClient remoteClient;
  private final RetryStrategy retryStrategy;
  private String basePath;

  @Inject
  public ArtifactLocalizer(DiscoveryServiceClient discoveryServiceClient) {
    this.remoteClient = new RemoteClient(discoveryServiceClient, Constants.Service.APP_FABRIC_HTTP,
                                         HttpRequestConfig.DEFAULT,
                                         Constants.Gateway.INTERNAL_API_VERSION_3);
    this.retryStrategy = RetryStrategies.exponentialDelay(100, 1000, TimeUnit.MILLISECONDS);
    this.basePath = "";
  }

  @VisibleForTesting
  public ArtifactLocalizer(DiscoveryServiceClient discoveryServiceClient, String basePath) {
    this(discoveryServiceClient);
    this.basePath = basePath;
  }

  /**
   * Gets the location on the local filesystem for the given artifact. This method handles fetching the artifact as well
   * as caching it.
   *
   * @param artifactId The ArtifactId of the artifact to fetch
   * @return The Local Location for this artifact
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception while fetching or caching the artifact
   * @throws Exception if there was an unexpected error
   */
  public File getArtifact(ArtifactId artifactId) throws Exception {
    LOG.debug("Fetching artifact info for {}", artifactId);
    Long lastModifiedTimestamp = getCurrentLastModifiedTimestamp(artifactId);

    File newJarLocation = Retries
      .callWithRetries(() -> fetchArtifact(artifactId, getURL(artifactId), lastModifiedTimestamp), retryStrategy);

    // If the lastModifiedTimestamp is not null then we might need to delete the old cache.
    if (lastModifiedTimestamp != null) {
      // TODO (CDAP-18051): Migrate this cleanup task to its own service
      deleteOldCache(artifactId, lastModifiedTimestamp, newJarLocation);
    }
    return newJarLocation;
  }

  /**
   * Gets the location on the local filesystem for the directory that contains the unpacked artifact. This method
   * handles fetching, caching and unpacking the artifact.
   *
   * @param artifactId The ArtifactId of the artifact to fetch and unpack
   * @return The Local Location of the directory that contains the unpacked artifact files
   * @throws ArtifactNotFoundException if the given artifact does not exist
   * @throws IOException if there was an exception while fetching, caching or unpacking the artifact
   * @throws Exception if there was an unexpected error
   */
  public File getAndUnpackArtifact(ArtifactId artifactId) throws Exception {
    LOG.debug("Unpacking artifact {}", artifactId);
    File jarLocation = getArtifact(artifactId);
    File unpackDir = getUnpackLocalPath(artifactId, Long.valueOf(jarLocation.getName().split("\\.")[0]));
    LOG.debug("Got unpack directory as {}", unpackDir);
    if (!unpackDir.exists()) {
      LOG.debug("Unpack directory doesn't exist, creating it now");
      BundleJarUtil.unJar(jarLocation, unpackDir);
    }
    return unpackDir;
  }

  /**
   * fetchArtifact attempts to connect to app fabric to download the given artifact. This method will throw {@link
   * RetryableException} in certain circumstances so using this with the
   */
  private File fetchArtifact(ArtifactId artifactId, String url,
                             Long lastModifiedTimestamp) throws IOException, ArtifactNotFoundException {
    HttpURLConnection urlConn = remoteClient.openConnection(HttpMethod.GET, url);
    if (lastModifiedTimestamp != null) {
      LOG.debug("Found existing local version with timestamp {}", lastModifiedTimestamp);

      ZonedDateTime lastModifiedDate = ZonedDateTime
        .ofInstant(Instant.ofEpochMilli(lastModifiedTimestamp), ZoneId.systemDefault());
      urlConn.setRequestProperty(HttpHeaderNames.IF_MODIFIED_SINCE, lastModifiedDate.format(DATE_TIME_FORMATTER));
    }

    try {
      // If we get this response that means we already have the most up to date artifact
      if (urlConn.getResponseCode() == HttpURLConnection.HTTP_NOT_MODIFIED) {
        LOG.debug("Call to app fabric returned NOT_MODIFIED");
        return getArtifactJarLocation(artifactId, lastModifiedTimestamp);
      }

      throwIfError(urlConn, artifactId);

      ZonedDateTime newModifiedDate = getLastModifiedHeader(urlConn);
      Long newTimestamp = newModifiedDate.toInstant().toEpochMilli();
      File newLocation = getArtifactJarLocation(artifactId, newTimestamp);

      try (InputStream in = urlConn.getInputStream()) {
        FileUtils.copyInputStreamToFile(in, newLocation);
      }
      return newLocation;
    } finally {
      urlConn.disconnect();
    }
  }

  private String getURL(ArtifactId artifactId) {
    String namespaceId = artifactId.getNamespace();
    ArtifactScope scope = ArtifactScope.USER;
    // Cant use 'system' as the namespace in the request because that generates an error, the namespace doesnt matter
    // as long as it exists. Using default because it will always be there
    if (ArtifactScope.SYSTEM.toString().equalsIgnoreCase(namespaceId)) {
      namespaceId = "default";
      scope = ArtifactScope.SYSTEM;
    }

    return String.format("namespaces/%s/artifacts/%s/versions/%s/download?scope=%s",
                         namespaceId,
                         artifactId.getArtifact(),
                         artifactId.getVersion(),
                         scope);
  }

  /**
   * This checks the local cache for this artifact and retrieves the timestamp for the newest cache entry, if this
   * artifact is not cached it returns null
   */
  @Nullable
  private Long getCurrentLastModifiedTimestamp(ArtifactId artifactId) {
    File artifactDir = getArtifactDirLocation(artifactId);

    // If the local cache exists, check if we have a timestamp directory
    if (artifactDir.exists()) {
      String[] fileList = artifactDir.list();
      if (fileList != null && fileList.length > 0) {
        // Split on period to get timestamp without the .jar extension (see full filepath in class JavaDoc)
        return Arrays.stream(fileList).map(s -> Long.valueOf(s.split("\\.")[0]))
          .max(Long::compare).get();
      }
    }
    return null;
  }

  /**
   * Helper method for handling the deletion of the old cache files, if required. This will also delete the directory
   * containing the unpacked contents of the out-of-date jar.
   */
  private void deleteOldCache(ArtifactId artifactId, Long lastModifiedTimestamp, File newJarLocation) {
    // This means we already have a jar but its out of date, we should delete the jar and the unpacked directory
    File oldJarLocation = getArtifactJarLocation(artifactId, lastModifiedTimestamp);
    LOG.debug("Got new location as {} with old location as {}", newJarLocation, oldJarLocation);
    if (!newJarLocation.equals(oldJarLocation)) {
      File oldUnpackLocation = getUnpackLocalPath(artifactId, lastModifiedTimestamp);
      LOG.debug("Deleting previously cached jar");
      try {
        oldJarLocation.delete();
        FileUtils.deleteDirectory(oldUnpackLocation);
      } catch (IOException e) {
        //Catch and log the exception, this should not cause the operation to fail
        LOG.warn("Failed to delete old cached jar for artifact {} version {}: {}", artifactId.getArtifact(),
                 artifactId.getVersion(), e);
      }
    }
  }

  /**
   * Helper function for verifying, extracting and converting the Last-Modified header from the URL connection.
   */
  private ZonedDateTime getLastModifiedHeader(HttpURLConnection urlConn) {
    Map<String, List<String>> headers = urlConn.getHeaderFields();
    if (!headers.containsKey(LAST_MODIFIED_HEADER) ||
      headers.get(LAST_MODIFIED_HEADER).size() != 1) {
      //Not sure how this would happen since this endpoint should always set the header.
      // If it does happen we should retry.
      throw new RetryableException(String.format("The response from %s did not contain the %s header.",
                                                 urlConn.getURL(), LAST_MODIFIED_HEADER));
    }

    return LocalDateTime
      .parse(headers.get(LAST_MODIFIED_HEADER).get(0), DATE_TIME_FORMATTER)
      .atZone(ZoneId.systemDefault());
  }

  /**
   * Helper method for catching and throwing any errors that might have occurred when attempting to connect.
   */
  private void throwIfError(HttpURLConnection urlConn, ArtifactId artifactId) throws IOException,
    ArtifactNotFoundException {
    int responseCode = urlConn.getResponseCode();
    if (responseCode == HttpURLConnection.HTTP_OK) {
      return;
    }

    String errMsg = CharStreams.toString(new InputStreamReader(urlConn.getErrorStream(), StandardCharsets.UTF_8));
    switch (responseCode) {
      case HttpURLConnection.HTTP_NOT_FOUND:
        throw new ArtifactNotFoundException(artifactId);
      case HttpURLConnection.HTTP_UNAVAILABLE:
        throw new ServiceUnavailableException(Constants.Service.APP_FABRIC_HTTP, errMsg);
    }
    throw new IOException(
      String.format("Failed to fetch artifact %s from app-fabric due to %s", artifactId, errMsg));
  }

  private Path getLocalPath(String dirName, ArtifactId artifactId) {
    return Paths.get(basePath, PD_DIR, dirName, artifactId.getNamespace(), artifactId.getArtifact(),
                     artifactId.getVersion());
  }

  /**
   * Returns a {@link File} representing the cache directory jars for the given artifact. The file path is:
   * /PD_DIRECTORY/artifacts/<namespace>/<artifact-name>/<artifact-version>/
   */
  private File getArtifactDirLocation(ArtifactId artifactId) {
    return getLocalPath("artifacts", artifactId).toFile();
  }

  /**
   * Returns a {@link File} representing the cached jar for the given artifact and timestamp. The file path is:
   * /PD_DIRECTORY/artifacts/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>.jar
   */
  private File getArtifactJarLocation(ArtifactId artifactId, @NotNull Long lastModifiedTimestamp) {
    return getLocalPath("artifacts", artifactId).resolve(String.format("%d.jar", lastModifiedTimestamp)).toFile();
  }

  /**
   * Returns a {@link File} representing the directory containing the unpacked contents of the jar for the given
   * artifact and timestamp. The file path is: /PD_DIRECTORY/unpacked/<namespace>/<artifact-name>/<artifact-version>/<last-modified-timestamp>
   */
  private File getUnpackLocalPath(ArtifactId artifactId, @NotNull Long lastModifiedTimestamp) {
    return getLocalPath("unpacked", artifactId).resolve(lastModifiedTimestamp.toString()).toFile();
  }
}
