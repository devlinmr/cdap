/*
 *
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.graphql.store.application.runtimewiring;

import com.google.inject.Inject;
import graphql.schema.idl.TypeRuntimeWiring;
import io.cdap.cdap.graphql.store.application.datafetchers.ApplicationRecordDataFetcher;
import io.cdap.cdap.graphql.store.application.schema.ApplicationFields;
import io.cdap.cdap.graphql.store.application.schema.ApplicationTypes;
import io.cdap.cdap.graphql.typeruntimewiring.CDAPTypeRuntimeWiring;

/**
 * ApplicationQuery type runtime wiring. Registers the data fetchers for the ApplicationQuery type.
 */
public class ApplicationQueryTypeRuntimeWiring implements CDAPTypeRuntimeWiring {

  private final ApplicationRecordDataFetcher applicationRecordDataFetcher;

  @Inject
  public ApplicationQueryTypeRuntimeWiring() {
    this.applicationRecordDataFetcher = ApplicationRecordDataFetcher.getInstance();
  }

  @Override
  public TypeRuntimeWiring getTypeRuntimeWiring() {
    return TypeRuntimeWiring.newTypeWiring(ApplicationTypes.APPLICATION_QUERY)
      .dataFetcher(ApplicationFields.APPLICATIONS, applicationRecordDataFetcher.getApplicationsDataFetcher())
      .build();
  }

}
