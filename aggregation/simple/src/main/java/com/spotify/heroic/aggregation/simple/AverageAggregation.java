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

import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.common.Sampling;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true, of = { "NAME" })
public class AverageAggregation extends BucketAggregation<Point, StripedAverageBucket> {
    public static final String NAME = "average";

    public AverageAggregation(Sampling sampling) {
        super(sampling, Point.class, MetricType.POINT);
    }

    @JsonCreator
    public static AverageAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new AverageAggregation(sampling);
    }

    @Override
    protected StripedAverageBucket buildBucket(long timestamp) {
        return new StripedAverageBucket(timestamp);
    }

    @Override
    protected Metric build(StripedAverageBucket bucket) {
        final long count = bucket.count();

        if (count == 0) {
            return Metric.invalid();
        }

        return new Point(bucket.timestamp(), bucket.value() / count);
    }
}