/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.security.auth.context;

import com.google.inject.Inject;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.security.auth.AccessToken;
import io.cdap.cdap.security.auth.AccessTokenCodec;
import io.cdap.cdap.security.auth.TokenManager;
import io.cdap.cdap.security.auth.UserIdentity;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;

import java.io.IOException;
import java.util.Base64;
import java.util.Collections;

public class SystemAuthenticationContext implements AuthenticationContext {
  public static final String SYSTEM_IDENTITY = "system";
  // Default expiration time of 5 minutes.
  private static final long DEFAULT_EXPIRATION = 300000;

  private final AccessTokenCodec accessTokenCodec;
  private final TokenManager tokenManager;

  @Inject
  SystemAuthenticationContext(AccessTokenCodec accessTokenCodec, TokenManager tokenManager) {
    this.accessTokenCodec = accessTokenCodec;
    this.tokenManager = tokenManager;
  }

  @Override
  public Principal getPrincipal() {
    long currentTimestamp = System.currentTimeMillis();
    UserIdentity identity = new UserIdentity(SYSTEM_IDENTITY, Collections.emptyList(), currentTimestamp,
                                             currentTimestamp + DEFAULT_EXPIRATION);
    AccessToken accessToken = tokenManager.signIdentifier(identity);
    String encodedAccessToken;
    try {
      encodedAccessToken = Base64.getEncoder().encodeToString(accessTokenCodec.encode(accessToken));
      return new Principal(SYSTEM_IDENTITY, Principal.PrincipalType.INTERNAL, encodedAccessToken);
    } catch (IOException e) {
      throw new RuntimeException("Unexpected failure while creating internal system identity", e);
    }
  }
}
