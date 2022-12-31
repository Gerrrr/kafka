/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class CachedMonotonicTime implements Time {
    private static final Logger LOG = Logger.getLogger(CachedMonotonicTime.class.getName());

    private volatile long currentMs;
    private volatile long currentNanos;

    private final long updateDelay;
    private final TimeUnit updateDelayUnit;

    private final Time timeSource;

    private final ScheduledExecutorService timeUpdaterService =
        Executors.newScheduledThreadPool(1);
    private ScheduledFuture<?> timeUpdater = null;

    CachedMonotonicTime(final Time timeSource) {
        this.timeSource = timeSource;

        this.currentMs = timeSource.milliseconds();
        this.currentNanos = timeSource.nanoseconds();

        // TODO: add config
        this.updateDelay = 5;
        this.updateDelayUnit = TimeUnit.MILLISECONDS;
    }

    CachedMonotonicTime startTimeTracking() {
        if (timeUpdater != null) {
            throw new IllegalStateException("Time tracking already started");
        }
        timeUpdater = timeUpdaterService.scheduleWithFixedDelay(
            this::updateTime,
            0,
            this.updateDelay,
            this.updateDelayUnit
        );
        return this;
    }

    CachedMonotonicTime stopTimeTracking() {
        if (timeUpdater == null) {
            throw new IllegalStateException("Time tracking already stopped");
        }
        timeUpdater.cancel(true);
        try {
            timeUpdater.get();
        } catch (CancellationException e) {
            // expected
        } catch (InterruptedException | ExecutionException e) {
            LOG.warning("Caught an exception while stopping time tracking: " + e);
        }
        timeUpdater = null;
        return this;
    }

    private void updateTime() {
        synchronized (this) {
            currentMs = Math.max(timeSource.milliseconds(), currentMs);
            currentNanos = timeSource.nanoseconds();
        }
    }

    @Override
    public long milliseconds() {
        return currentMs;
    }

    @Override
    public long nanoseconds() {
        return currentNanos;
    }

    @Override
    public void sleep(final long ms) {
        Utils.sleep(ms);
    }
}
