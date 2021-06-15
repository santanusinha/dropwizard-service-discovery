/*
 * Copyright (c) 2019 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.appform.dropwizard.discovery.bundle;

import lombok.experimental.UtilityClass;

/**
 * Constants
 */
@UtilityClass
public class Constants {
    public final String DEFAULT_NAMESPACE = "default";
    public final String DEFAULT_HOST = "__DEFAULT_SERVICE_HOST";
    public final int DEFAULT_PORT = -1;
    public final int DEFAULT_DW_CHECK_INTERVAl = 15;
    public final int DEFAULT_RETRY_CONN_INTERVAL = 5000;

    /**
     *  Zones and Node specific constants
     */
    public final int DEFAULT_ZONE_ID = 0;
    public final int MAX_ZONES = 10;
    public final int MAX_NODES_PER_ZONE = 1000;

    /**
     * Id specific constants
     */
    public final int MAX_ID_PER_MS = 1000;
}