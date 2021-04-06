/*
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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ExecutionGraphInfoStore;
import org.apache.flink.runtime.dispatcher.MemoryExecutionGraphInfoStore;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.ArbitraryWorkerResourceSpecFactory;
import org.apache.flink.runtime.resourcemanager.JobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.ResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServices;
import org.apache.flink.runtime.resourcemanager.ResourceManagerRuntimeServicesConfiguration;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.resourcemanager.TestingJobLeaderIdService;
import org.apache.flink.runtime.resourcemanager.TestingResourceManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.ConfigurationException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for the {@link ClusterEntrypoint}. */
public class ClusterEntrypointTest {

    private static final long TIMEOUT_MS = 3000;

    private Configuration flinkConfig;
    private ExecutorService testingExecutor;

    @Before
    public void before() {
        flinkConfig = new Configuration();
        testingExecutor =
                Executors.newSingleThreadExecutor(new ExecutorThreadFactory("testing-executor"));
    }

    @After
    public void tearDown() {
        testingExecutor.shutdownNow();
    }

    @Test(expected = IllegalConfigurationException.class)
    public void testStandaloneSessionClusterEntrypointDeniedInReactiveMode() {
        flinkConfig.set(JobManagerOptions.SCHEDULER_MODE, SchedulerExecutionMode.REACTIVE);
        new TestingEntryPoint.Builder().setConfiguration(flinkConfig).build();
        fail("Entrypoint initialization is supposed to fail");
    }

    @Test
    public void testCloseAsyncShouldNotCleanUpHAData() throws Exception {
        final CompletableFuture<Void> closeFuture = new CompletableFuture<>();
        final CompletableFuture<Void> closeAndCleanupAllDataFuture = new CompletableFuture<>();
        final HighAvailabilityServices testingHaService =
                new TestingHighAvailabilityServicesBuilder()
                        .setCloseFuture(closeFuture)
                        .setCloseAndCleanupAllDataFuture(closeAndCleanupAllDataFuture)
                        .build();
        final TestingEntryPoint testingEntryPoint =
                new TestingEntryPoint.Builder()
                        .setConfiguration(flinkConfig)
                        .setHighAvailabilityServices(testingHaService)
                        .build();

        final CompletableFuture<ApplicationStatus> appStatusFuture =
                startClusterEntrypoint(testingEntryPoint);

        testingEntryPoint.closeAsync();
        assertThat(
                appStatusFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS),
                is(ApplicationStatus.UNKNOWN));
        assertThat(closeFuture.isDone(), is(true));
        assertThat(closeAndCleanupAllDataFuture.isDone(), is(false));
    }

    @Test
    public void testCloseAsyncShouldNotDeregisterApp() throws Exception {
        final CompletableFuture<Void> deregisterFuture = new CompletableFuture<>();
        final TestingResourceManagerFactory testingResourceManagerFactory =
                new TestingResourceManagerFactory.Builder()
                        .setInternalDeregisterApplicationConsumer(
                                (ignored1, ignored2) -> deregisterFuture.complete(null))
                        .build();
        final TestingEntryPoint testingEntryPoint =
                new TestingEntryPoint.Builder()
                        .setConfiguration(flinkConfig)
                        .setResourceManagerFactory(testingResourceManagerFactory)
                        .build();

        final CompletableFuture<ApplicationStatus> appStatusFuture =
                startClusterEntrypoint(testingEntryPoint);

        testingEntryPoint.closeAsync();
        assertThat(
                appStatusFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS),
                is(ApplicationStatus.UNKNOWN));
        assertThat(deregisterFuture.isDone(), is(false));
    }

    private CompletableFuture<ApplicationStatus> startClusterEntrypoint(
            TestingEntryPoint testingEntryPoint) throws Exception {
        testingEntryPoint.startCluster();
        return FutureUtils.supplyAsync(
                () -> testingEntryPoint.getTerminationFuture().get(), testingExecutor);
    }

    private static class TestingEntryPoint extends ClusterEntrypoint {

        private final HighAvailabilityServices haService;

        private final ResourceManagerFactory<ResourceID> resourceManagerFactory;

        private TestingEntryPoint(
                Configuration configuration,
                HighAvailabilityServices haService,
                ResourceManagerFactory<ResourceID> resourceManagerFactory) {
            super(configuration);
            SignalHandler.register(LOG);
            this.haService = haService;
            this.resourceManagerFactory = resourceManagerFactory;
        }

        @Override
        protected DispatcherResourceManagerComponentFactory
                createDispatcherResourceManagerComponentFactory(Configuration configuration)
                        throws IOException {
            return DefaultDispatcherResourceManagerComponentFactory.createSessionComponentFactory(
                    resourceManagerFactory);
        }

        @Override
        protected ExecutionGraphInfoStore createSerializableExecutionGraphStore(
                Configuration configuration, ScheduledExecutor scheduledExecutor)
                throws IOException {
            return new MemoryExecutionGraphInfoStore();
        }

        @Override
        protected HighAvailabilityServices createHaServices(
                Configuration configuration, Executor executor) {
            return haService;
        }

        @Override
        protected boolean supportsReactiveMode() {
            return false;
        }

        public static final class Builder {
            private HighAvailabilityServices haService =
                    new TestingHighAvailabilityServicesBuilder().build();

            private ResourceManagerFactory<ResourceID> resourceManagerFactory =
                    StandaloneResourceManagerFactory.getInstance();

            private Configuration configuration = new Configuration();

            public Builder setHighAvailabilityServices(HighAvailabilityServices haService) {
                this.haService = haService;
                return this;
            }

            public Builder setResourceManagerFactory(
                    ResourceManagerFactory<ResourceID> resourceManagerFactory) {
                this.resourceManagerFactory = resourceManagerFactory;
                return this;
            }

            public Builder setConfiguration(Configuration configuration) {
                this.configuration = configuration;
                return this;
            }

            public TestingEntryPoint build() {
                return new TestingEntryPoint(configuration, haService, resourceManagerFactory);
            }
        }
    }

    private static class TestingResourceManagerFactory extends ResourceManagerFactory<ResourceID> {

        private final BiConsumer<ApplicationStatus, String> deregisterAppConsumer;

        private TestingResourceManagerFactory(
                BiConsumer<ApplicationStatus, String> deregisterAppConsumer) {
            this.deregisterAppConsumer = deregisterAppConsumer;
        }

        @Override
        protected ResourceManager<ResourceID> createResourceManager(
                Configuration configuration,
                ResourceID resourceId,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                FatalErrorHandler fatalErrorHandler,
                ClusterInformation clusterInformation,
                @Nullable String webInterfaceUrl,
                ResourceManagerMetricGroup resourceManagerMetricGroup,
                ResourceManagerRuntimeServices resourceManagerRuntimeServices,
                Executor ioExecutor)
                throws Exception {
            final SlotManager slotManager =
                    SlotManagerBuilder.newBuilder()
                            .setScheduledExecutor(rpcService.getScheduledExecutor())
                            .build();
            final JobLeaderIdService jobLeaderIdService =
                    new TestingJobLeaderIdService.Builder().build();
            return new TestingResourceManager(
                    rpcService,
                    resourceId,
                    highAvailabilityServices,
                    heartbeatServices,
                    slotManager,
                    NoOpResourceManagerPartitionTracker::get,
                    jobLeaderIdService,
                    fatalErrorHandler,
                    resourceManagerMetricGroup) {
                @Override
                protected void internalDeregisterApplication(
                        ApplicationStatus finalStatus, @Nullable String diagnostics) {
                    deregisterAppConsumer.accept(finalStatus, diagnostics);
                }
            };
        }

        @Override
        protected ResourceManagerRuntimeServicesConfiguration
                createResourceManagerRuntimeServicesConfiguration(Configuration configuration)
                        throws ConfigurationException {
            return ResourceManagerRuntimeServicesConfiguration.fromConfiguration(
                    configuration, ArbitraryWorkerResourceSpecFactory.INSTANCE);
        }

        public static final class Builder {
            private BiConsumer<ApplicationStatus, String> deregisterAppConsumer =
                    (ignore1, ignore2) -> {};

            public Builder setInternalDeregisterApplicationConsumer(
                    BiConsumer<ApplicationStatus, String> biConsumer) {
                this.deregisterAppConsumer = biConsumer;
                return this;
            }

            public TestingResourceManagerFactory build() {
                return new TestingResourceManagerFactory(deregisterAppConsumer);
            }
        }
    }
}
