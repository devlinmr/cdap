/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.http.handlers;

import com.continuuity.common.http.core.HandlerContext;
import com.continuuity.common.http.core.HttpHandler;
import com.continuuity.common.http.core.HttpResponder;
import com.continuuity.passport.PassportConstants;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.RolesAccounts;
import com.continuuity.passport.meta.VPC;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import java.util.List;

/**
 * Defines End point for vpc related functions.
 */

@Path("/passport/v1/vpc")
@Singleton
public class VPCHandler extends PassportHandler implements HttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(VPCHandler.class);
  private final DataManagementService dataManagementService;

  @Inject
  public VPCHandler(DataManagementService dataManagementService) {
    this.dataManagementService = dataManagementService;
  }

  @GET
  @Produces("application/json")
  public void getVPC(HttpRequest request, HttpResponder responder) {
    try {
      requestReceived();
      String apiKey = request.getHeader(PassportConstants.CONTINUUITY_API_KEY_HEADER);
      List<VPC> vpcList = dataManagementService.getVPC(apiKey);
      if (vpcList.isEmpty()) {
        responder.sendString(HttpResponseStatus.OK, "[]");
      } else {
        StringBuilder returnJson = new StringBuilder();
        returnJson.append("[");
        boolean first = true;
        for (VPC vpc : vpcList) {
          if (first) {
            first = false;
          } else {
            returnJson.append(",");
          }
          returnJson.append(vpc.toString());
        }
        returnJson.append("]");
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, returnJson.toString());
      }
    } catch (Exception e) {
      requestFailed();
      LOG.error(String.format("Internal server error processing endpoint: %s %s",
        "GET /passport/v1/vpc}", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJsonError(String.format("VPC get Failed. %s", e)));
    }
  }

  @Path("valid/{vpcName}")
  @GET
  public void isValidVPC(HttpRequest request, HttpResponder responder, @PathParam("vpcName") String vpcName) {
    try {
      if (dataManagementService.isValidVPC(vpcName)) {
        responder.sendString(HttpResponseStatus.OK, Utils.getJsonOK());
      } else {
        responder.sendString(HttpResponseStatus.OK, Utils.getJsonError("VPC already exists"));
      }
    } catch (Exception e) {
      requestFailed();
      LOG.error(String.format("Internal server error processing endpoint: %s %s",
                              "GET /passport/v1/vpc/valid/{vpcName}", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR, Utils.getJsonError("FAILED", e.getMessage()));
    }
  }

  @Path("{vpcName}")
  @GET
  public void getAccountForVPCName(HttpRequest request, HttpResponder responder,
                                   @PathParam("vpcName") String vpcName) {
    try {
      Account account = dataManagementService.getAccountForVPC(vpcName);
      if (account != null) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, account.toString());
      } else {
        requestFailed();
        LOG.error(String.format("Account not found. Processing endpoint: %s ", "GET /passport/v1/vpc/{vpcName}"));
        responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getJsonError("Account not found for VPC"));
      }
    } catch (Exception e) {
      requestFailed();
      LOG.error(String.format("Account not found. endpoint: %s %s", "GET /passport/v1/vpc/{vpcName}", e.getMessage()));
      responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getJsonError("Account not found for VPC"));
    }
  }

  /**
   * Gets account if for VPC.
   * Endpoint is obfuscated on purpose
   * @param vpcName VpcName
   * @return Instance of {@code Response}
   */
  @Path("xkcd/{vpcName}")
  @GET
  public void getIdForVPC(HttpRequest request, HttpResponder responder,
                          @PathParam("vpcName") String vpcName) {
    try {
      Account account = dataManagementService.getAccountForVPC(vpcName);
      if (account != null) {
        requestSuccess();
        responder.sendString(HttpResponseStatus.OK, Utils.getIdJson(null, account.getAccountId()));
      } else {
        requestFailed();
        LOG.error(String.format("xkcd not found. Processing endpoint: %s ",
                                "GET /passport/v1/vpc/xkcd/{vpcName}"));
        responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getIdJson("FAILED", "xkcd not found for VPC"));
      }
    } catch (Exception e) {
      requestFailed();
      LOG.error(String.format("xkcd not found. endpoint: %s %s", "GET /passport/v1/xkcd/{vpcName}", e.getMessage()));
      responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getIdJson("FAILED", "xkcd not found for VPC"));
    }
  }

  @GET
  @Path("{vpcName}/accountRoles")
  public void getAccountRoles(HttpRequest request, HttpResponder responder,
                              @PathParam("vpcName") String vpcName) {
    requestReceived();
    JsonArray accountRoleArray = new JsonArray();
    try {
      RolesAccounts rolesAccounts = dataManagementService.getAccountRoles(vpcName);
      for (String role : rolesAccounts.getRoles()) {
        JsonObject entry = new JsonObject();
        JsonArray accountArray = new JsonArray();
        for (Account account : rolesAccounts.getAccounts(role)){
          accountArray.add(account.toJson());
        }
        entry.addProperty("role", role);
        entry.add("accounts", accountArray);
        accountRoleArray.add(entry);
      }
      requestSuccess();
      responder.sendString(HttpResponseStatus.OK, accountRoleArray.toString());
    } catch (Exception e) {
      requestFailed();
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJsonError("VPC delete Failed", e.getMessage()));
    }
   }

  @Path("{vpcName}")
  @DELETE
  public void deleteVPCByName(HttpRequest request, HttpResponder responder,
                              @PathParam("vpcName") String vpcName) {
    try {
      requestSuccess();
      dataManagementService.deleteVPC(vpcName);
      responder.sendString(HttpResponseStatus.OK, Utils.getJsonOK());
    } catch (VPCNotFoundException e) {
      requestFailed(); //Failed request
      LOG.debug(String.format("VPC not found endpoint: %s %s",
        "DELETE /passport/v1/vpc/{vpcName}", e.getMessage()));
      responder.sendString(HttpResponseStatus.NOT_FOUND, Utils.getJsonError("VPC not found"));
    } catch (RuntimeException e) {
      requestFailed(); //Failed request
      LOG.error(String.format("Internal server error endpoint: %s %s",
        "DELETE /passport/v1/{vpcName}", e.getMessage()));
      responder.sendString(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                           Utils.getJsonError("VPC delete failed", e.getMessage()));
    }
  }
  @Override
  public void init(HandlerContext context) {
  }

  @Override
  public void destroy(HandlerContext context) {
  }
}
