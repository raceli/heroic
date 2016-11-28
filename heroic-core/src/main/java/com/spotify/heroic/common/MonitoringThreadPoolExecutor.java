/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.common;

import static java.lang.System.nanoTime;

import java.lang.ref.WeakReference;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MonitoringThreadPoolExecutor extends ThreadPoolExecutor {
    BlockingQueue<Runnable> workQueue;
    int maximumPoolSize;
    ConcurrentHashMap<Long, ThreadStatistics> threadStats = new ConcurrentHashMap<>();
    ConcurrentHashMap<Runnable, RunnableStatistics> runnableStats = new ConcurrentHashMap<>();
    LogThread logThread;


    class LogThread extends Thread {
        WeakReference<MonitoringThreadPoolExecutor> weakReference;

        public LogThread(MonitoringThreadPoolExecutor parent) {
            weakReference = new WeakReference<MonitoringThreadPoolExecutor>(parent);
        }

        public void run() {
            while (true) {
                MonitoringThreadPoolExecutor parent = weakReference.get();
                if (parent == null) {
                    // Parent is gone, exit thread
                    log.info("Parent gone, exiting MonitoringThreadPoolExecutor thread");
                    return;
                }

                parent.logStatistics();

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                }
            }
        }
    }


    public MonitoringThreadPoolExecutor(int corePoolSize,
                                        int maximumPoolSize,
                                        long keepAliveTime,
                                        TimeUnit unit,
                                        BlockingQueue<Runnable> workQueue,
                                        ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        this.workQueue = workQueue;
        this.maximumPoolSize = maximumPoolSize;
        logThread = new LogThread(this);
        logThread.start();
    }

    protected void beforeExecute(Thread thread, Runnable r) {
        long currTime = nanoTime();
        ThreadStatistics ts = threadStats.get(thread.getId());
        if (ts == null) {
            // First ever execution in this thread
            ts = new ThreadStatistics(0, 0, 0, true);
            threadStats.put(thread.getId(), ts);
        } else {
            ts.setTotalSleepTime( ts.getTotalSleepTime() + (currTime - ts.getLastExecutionEnd()) );
            ts.setCurrentlyRunning(true);
        }

        RunnableStatistics rs = new RunnableStatistics(thread.getId(), currTime);
        runnableStats.put(r, rs);
    }

    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);


        long currTime = nanoTime();
        RunnableStatistics rs = runnableStats.get(r);
        if (rs == null) {
            // Should not happen
            log.info("  Had no entry in runnableStats - should not happen!");
            return;
        }
        runnableStats.remove(r);

        ThreadStatistics ts = threadStats.get(rs.getThreadId());
        if (ts == null) {
            // Should not happen
            log.info("  Had no entry in threadStats - should not happen!");
            return;
        }

        ts.setTotalExecutionTime(ts.getTotalExecutionTime() + (currTime - rs.getExecutionStart()));
        ts.setLastExecutionEnd(currTime);
        ts.setCurrentlyRunning(false);
    }

    public void logStatistics() {
        long currTime = nanoTime();
        Enumeration<Long> keys = threadStats.keys();
        log.info("Statistics for Thread Pool. maximumPoolSize:" + maximumPoolSize + " queueLength:" + workQueue.size() + " threadStats.size:" + threadStats.size() + " runnableStats.size:" + runnableStats.size());
        while (keys.hasMoreElements()) {
            long threadId = keys.nextElement();
            ThreadStatistics ts = threadStats.get(threadId);
            long sleepTime = ts.getTotalSleepTime();
            if (ts.currentlyRunning == false && ts.getLastExecutionEnd() != 0) {
                sleepTime += currTime - ts.getLastExecutionEnd();
            }
            log.info("  " + threadId + ": currentlyRunning:" + ts.currentlyRunning +
                     " totalExecutionTime:" + (ts.getTotalExecutionTime()/1000) +
                     " totalSleepTime:" + (sleepTime/1000) +
                     "(" + (ts.getTotalSleepTime()/1000) + ")");
        }
    }

    @AllArgsConstructor
    @Data
    class ThreadStatistics {
        long totalSleepTime;
        long totalExecutionTime;
        long lastExecutionEnd;
        boolean currentlyRunning;
    };

    @AllArgsConstructor
    @Data
    class RunnableStatistics {
        long threadId;
        long executionStart;
    };
};
