/*
 * Copyright (c) 2016 Spotify AB.
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

package com.spotify.heroic;

import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.MetricCollection;
import dagger.Module;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;
import lombok.AllArgsConstructor;
import lombok.Data;

import lombok.extern.slf4j.Slf4j;

@Module
@Slf4j
public class QueryLifecycleMonitor extends Thread {
    static final boolean LOG_OBJECT_DESTRUCTION = false;
    static final boolean PERIODIC_LOG_STATISTICS = true;
    static final boolean PERIODIC_LOG_FULL_LIST = false;
    static final boolean PERIODIC_LOG_METRIC_QUERY_STATS = true;
    static final boolean PERIODIC_LOG_METRIC_QUERY_TOP5 = true;

    private final CountDownLatch pleaseStop = new CountDownLatch(1);

    private ConcurrentHashMap<Object, ObjectMetadata> metricCollectionRegistry = new ConcurrentHashMap<>();
    private ReferenceQueue<Object> mcDeadReferenceQueue = new ReferenceQueue<>();

    private ConcurrentHashMap<Object, UUID> queryIdRegistry = new ConcurrentHashMap<>();
    private ConcurrentHashMap<UUID, Object> queryIdToWeakReferenceRegistry = new ConcurrentHashMap<>();
    private ReferenceQueue<Object> queryDeadReferenceQueue = new ReferenceQueue<>();

    private LongAdder totalByteCount = new LongAdder();
    private long maxStronglyReferencedTime;

    public QueryLifecycleMonitor() {
        start();
    }

    public void registerMetricCollection(MetricCollection object, String description, long numBytes) {
        ObjectMetadata m = new ObjectMetadata(description, numBytes, System.currentTimeMillis(), 0, 0);

        WeakReference<MetricCollection> weakReference =
            new WeakReference<MetricCollection>(object, mcDeadReferenceQueue);
        metricCollectionRegistry.put(weakReference, m);

        totalByteCount.add(numBytes);
    }

    public void registerFullQueryRequest(FullQuery.Request object) {
        WeakReference<FullQuery.Request> weakReference =
            new WeakReference<FullQuery.Request>(object, queryDeadReferenceQueue);
        queryIdRegistry.put(weakReference, object.getOriginContext().getQueryId());
        queryIdToWeakReferenceRegistry.put(object.getOriginContext().getQueryId(), weakReference);
    }


    private int checkMetricCollectionDeadReferences() {
        Object deadObject = mcDeadReferenceQueue.poll();
        if (deadObject == null) {
            return 0;
        }

        WeakReference<MetricCollection> deadReference = (WeakReference<MetricCollection>)deadObject;

        ObjectMetadata m = metricCollectionRegistry.get(deadReference);
        if (m == null) {
            return 0;
        }

        // An object has no strong references and will be GC:d soon
        m.setNoLongerReferencedTime(System.currentTimeMillis());

        if (LOG_OBJECT_DESTRUCTION) {
            log.info("Object is getting ready for GC: " + m.dumpToString());
        }

        totalByteCount.add( - m.numBytes);
        metricCollectionRegistry.remove(deadObject);
        return 1;
    }

    private int checkFullQueryRequestDeadReferences() {
        Object deadObject = queryDeadReferenceQueue.poll();
        if (deadObject == null) {
            return 0;
        }

        WeakReference<FullQuery.Request> deadReference = (WeakReference<FullQuery.Request>)deadObject;

        UUID id = queryIdRegistry.get(deadReference);
        if (id == null) {
            return 0;
        }

        // An object has no strong references and will be GC:d soon
        queryIdRegistry.remove(deadReference);
        queryIdToWeakReferenceRegistry.remove(id);
        return 1;
    }


    public void run() {
        long lastLogTime = System.currentTimeMillis();
        long logInterval = 10000;

        while (pleaseStop.getCount() == 1) {
            if (lastLogTime + logInterval < System.currentTimeMillis()) {
                periodicLogDump();
                lastLogTime = System.currentTimeMillis();
            }

            int processed = 0;
            processed += checkMetricCollectionDeadReferences();
            processed += checkFullQueryRequestDeadReferences();

            if (processed == 0) {
                // Avoid busy looping when there's no work
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
        }
    }

    void periodicLogDump() {
        long currTime = System.currentTimeMillis();
        if (PERIODIC_LOG_STATISTICS) {
            log.info("Tracked in-memory objects: num:" + metricCollectionRegistry.size() +
                     " totalBytes:" + totalByteCount.longValue());
        }
        if (PERIODIC_LOG_FULL_LIST) {
            Enumeration<ObjectMetadata> e = metricCollectionRegistry.elements();
            while (e.hasMoreElements()) {
                ObjectMetadata m = e.nextElement();
                log.info("  " + m.dumpToString(currTime));
            }
        }

        if (PERIODIC_LOG_METRIC_QUERY_STATS || PERIODIC_LOG_METRIC_QUERY_TOP5) {
            Map<UUID, QueryMetadata> queries = new HashMap<UUID, QueryMetadata>();
            long totalBytes = 0;

            Enumeration<Object> enumerator = metricCollectionRegistry.keys();
            while (enumerator.hasMoreElements()) {
                Object o = enumerator.nextElement();
                WeakReference<MetricCollection> weakReference = (WeakReference<MetricCollection>)o;
                MetricCollection metricCollection = weakReference.get();
                if (metricCollection == null) {
                    log.info("periodicLogDump() metricCollection was null");
                    continue;
                }
                ObjectMetadata objectMetadata = metricCollectionRegistry.get(weakReference);
                if (objectMetadata == null) {
                    log.info("periodicLogDump() objectMetadata was null");
                    continue;
                }

                // Summarize statistics for all queries being referenced by MetricCollection objects
                QueryOriginContext originContext = metricCollection.getOriginContext();
                if (originContext == null) {
                    log.info("periodicLogDump() originContext was null");
                    continue;
                }
                QueryMetadata queryMetadata = queries.get(originContext.getQueryId());
                if (queryMetadata == null) {
                    queryMetadata = new QueryMetadata(originContext, 0, 0, 0);
                    queries.put(originContext.getQueryId(), queryMetadata);
                }
                queryMetadata.setNumObjects( queryMetadata.getNumObjects() + 1);
                queryMetadata.setNumBytes( queryMetadata.getNumBytes() + objectMetadata.getNumBytes());
                totalBytes += objectMetadata.getNumBytes();

                if (queryMetadata.getOldestCreationTime() == 0 ||
                    objectMetadata.getCreationTime() < queryMetadata.getOldestCreationTime()) {
                    queryMetadata.setOldestCreationTime(objectMetadata.getCreationTime());
                }
            }

            if (PERIODIC_LOG_METRIC_QUERY_STATS) {
                log.info("In-flight queries: count:" + queries.keySet().size() +
                         " currentMemoryConsumptionOfQueryResults:" + totalBytes);
            }
            if (PERIODIC_LOG_METRIC_QUERY_TOP5) {
                List<UUID> orderedBySize = new ArrayList<UUID>(queries.keySet());
                orderedBySize.sort((o1, o2) -> {
                    long val1 = queries.get(o1).getNumBytes();
                    long val2 = queries.get(o2).getNumBytes();
                    if (val1 < val2) {
                        return -1;
                    }
                    if (val1 == val2) {
                        return 0;
                    }
                    return 1;
                });
                int maxQueriesToLog = 5;
                int index = 0;
                Iterator<UUID> idIterator = orderedBySize.iterator();
                log.info("Top live queries:");
                while (idIterator.hasNext() && index < maxQueriesToLog) {
                    QueryMetadata queryMetadata = queries.get(idIterator.next());
                    if (queryMetadata == null) {
                        continue;
                    }

                    FullQuery.Request fullQueryRequest = null;
                    Object weakReferenceObject =
                        queryIdToWeakReferenceRegistry.get(queryMetadata.getOriginContext().
                            getQueryId());
                    if (weakReferenceObject != null) {
                        WeakReference<FullQuery.Request> weakReference =
                            (WeakReference<FullQuery.Request>)weakReferenceObject;
                        fullQueryRequest = weakReference.get();
                    }
                    log.info("  [Query] bytesInMemory:" + queryMetadata.getNumBytes() +
                             " ID:" + queryMetadata.getOriginContext().getQueryId() +
                             " origin:" + queryMetadata.getOriginContext().getRemoteAddr() +
                             ":" + queryMetadata.getOriginContext().getRemotePort() +
                             " clientId:" + queryMetadata.getOriginContext().getRemoteClientId() +
                             " userAgent:" + queryMetadata.getOriginContext().getRemoteUserAgent() +
                             " ageOfOldestInMemoryData:" + (currTime -
                                                            queryMetadata.getOldestCreationTime()) +
                             "us" +
                             " fullQuery:{" + (fullQueryRequest == null ? "<unavailable>" :
                                               fullQueryRequest.toString()));
                    index++;
                }
            }
        }

    }

    @AllArgsConstructor
    @Data
    class ObjectMetadata {
        private String description;
        private long numBytes;
        private long creationTime;
        private long noLongerReferencedTime;
        private long finalizedTime;

        public String dumpToString() {
            return dumpToString(System.currentTimeMillis());
        }

        public String dumpToString(long currTime) {
            return "\"" + description + "\": bytes:" + numBytes +
                   " lifetime(ms):" + (currTime - creationTime) +
                   " stronglyReferenced:" +
                   (noLongerReferencedTime == 0 ?
                    "true" : "false weakReferenceTime:" + (currTime - noLongerReferencedTime));
        }
    }

    @AllArgsConstructor
    @Data
    class QueryMetadata {
        private QueryOriginContext originContext;
        private long numObjects;
        private long numBytes;
        private long oldestCreationTime;
    }
}
