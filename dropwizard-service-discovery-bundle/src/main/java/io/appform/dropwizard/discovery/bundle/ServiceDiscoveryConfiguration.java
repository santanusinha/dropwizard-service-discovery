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

import com.google.common.base.Strings;
import lombok.*;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Ranger configuration.
 */
@Data
@EqualsAndHashCode
@ToString
@NoArgsConstructor
public class ServiceDiscoveryConfiguration {
    @NotNull
    @NotEmpty
    private String namespace = Constants.DEFAULT_NAMESPACE;

    @NotNull
    @NotEmpty
    private String environment;

    @NotNull
    @NotEmpty
    private String zookeeper;

    @Min(1000)
    @Max(60000)
    private int connectionRetryIntervalMillis = Constants.DEFAULT_RETRY_CONN_INTERVAL;

    @NotNull
    @NotEmpty
    private String publishedHost = Constants.DEFAULT_HOST;

    @NotNull
    @Min(-1)
    @Max(65535)
    private int publishedPort = Constants.DEFAULT_PORT;

    private int refreshTimeMs;

    private boolean enableWatchers;

    @Min(0)
    @Max(600)
    @Deprecated
    private long initialDelaySeconds;

    private boolean initialRotationStatus = true;

    private int dropwizardCheckInterval = Constants.DEFAULT_DW_CHECK_INTERVAl;

    private int dropwizardCheckStaleness;

    @Builder
    public ServiceDiscoveryConfiguration(
            String namespace,
            String environment,
            String zookeeper,
            int connectionRetryIntervalMillis,
            String publishedHost,
            int publishedPort,
            int refreshTimeMs,
            boolean enableWatchers,
            long initialDelaySeconds,
            boolean initialRotationStatus,
            int dropwizardCheckInterval,
            int dropwizardCheckStaleness) {
        this.namespace = Strings.isNullOrEmpty(namespace)
                         ? Constants.DEFAULT_NAMESPACE
                         : namespace;
        this.environment = environment;
        this.zookeeper = zookeeper;
        this.connectionRetryIntervalMillis = connectionRetryIntervalMillis == 0
                                             ? Constants.DEFAULT_RETRY_CONN_INTERVAL
                                             : connectionRetryIntervalMillis;
        this.publishedHost = Strings.isNullOrEmpty(publishedHost)
                             ? Constants.DEFAULT_HOST
                             : publishedHost;
        this.publishedPort = publishedPort == 0
                             ? Constants.DEFAULT_PORT
                             : publishedPort;
        this.refreshTimeMs = refreshTimeMs;
        this.enableWatchers = enableWatchers;
        this.initialDelaySeconds = initialDelaySeconds;
        this.initialRotationStatus = initialRotationStatus;
        this.dropwizardCheckInterval = dropwizardCheckInterval == 0
                                       ? Constants.DEFAULT_DW_CHECK_INTERVAl
                                       : dropwizardCheckInterval;
        this.dropwizardCheckStaleness = dropwizardCheckStaleness;
    }
}
