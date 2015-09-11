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

package com.spotify.heroic.aggregation.simple;

import java.util.Map;

import lombok.Data;

import com.google.common.util.concurrent.AtomicDouble;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;

/**
 * A bucket implementation that retains the largest (max) value seen.
 *
 * @author udoprog
 */
@Data
public class MaxBucket implements DoubleBucket<Point> {
    private final long timestamp;
    private final AtomicDouble value = new AtomicDouble(Double.NEGATIVE_INFINITY);

    public long timestamp() {
        return timestamp;
    }

    @Override
    public void update(Map<String, String> tags, MetricType type, Point d) {
        while (true) {
            double current = value.get();

            if (current > d.getValue()) {
                break;
            }

            if (value.compareAndSet(current, d.getValue())) {
                break;
            }
        }
    }

    @Override
    public double value() {
        final double result = value.get();

        if (Double.isInfinite(result))
            return Double.NaN;

        return result;
    }
}