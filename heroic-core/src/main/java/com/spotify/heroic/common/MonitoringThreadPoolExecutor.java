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
        logThread = new LogThread(this);
        logThread.start();
    }

    public void beforeExecution(Thread thread, Runnable r) {
        long currTime = nanoTime();
        ThreadStatistics ts = threadStats.get(thread.getId());
        if (ts == null) {
            // First ever execution in this thread
            ts = new ThreadStatistics(0, 0, 0);
            threadStats.put(thread.getId(), ts);
        } else {
            ts.setTotalSleepTime( ts.getTotalSleepTime() + (currTime - ts.getLastExecutionEnd()) );
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
            return;
        }
        runnableStats.remove(rs);

        ThreadStatistics ts = threadStats.get(rs.getThreadId());
        if (ts == null) {
            // Should not happen
            return;
        }

        ts.setTotalExecutionTime(ts.getTotalExecutionTime() + (currTime - rs.getExecutionStart()));
    }

    public void logStatistics() {
        Enumeration<Long> keys = threadStats.keys();
        log.info("Statistics for Thread Pool. Queue Length:" + workQueue.size() + " threadStats.size:" + threadStats.size());
        while (keys.hasMoreElements()) {
            long threadId = keys.nextElement();
            ThreadStatistics ts = threadStats.get(threadId);
            log.info("  " + threadId + ": totalSleepTime:" + ts.getTotalSleepTime() + " totalExecutionTime:" + ts.getTotalExecutionTime());
        }
    }

    @AllArgsConstructor
    @Data
    class ThreadStatistics {
        long totalSleepTime;
        long totalExecutionTime;
        long lastExecutionEnd;
    };

    @AllArgsConstructor
    @Data
    class RunnableStatistics {
        long threadId;
        long executionStart;
    };
};
