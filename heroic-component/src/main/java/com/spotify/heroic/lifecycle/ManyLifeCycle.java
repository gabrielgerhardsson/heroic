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

package com.spotify.heroic.lifecycle;

import com.google.common.collect.ImmutableList;
import lombok.Data;

import java.util.List;

@Data
public class ManyLifeCycle implements LifeCycle {
    private final List<LifeCycle> lifeCycles;

    public static LifeCycle of(final Iterable<LifeCycle> many) {
        final ImmutableList.Builder<LifeCycle> flattened = ImmutableList.builder();

        for (final LifeCycle l : many) {
            if (l.equals(LifeCycle.EMPTY)) {
                continue;
            }

            if (l instanceof ManyLifeCycle) {
                flattened.addAll(((ManyLifeCycle) l).lifeCycles);
                continue;
            }

            flattened.add(l);
        }

        return new ManyLifeCycle(flattened.build());
    }

    @Override
    public String toString() {
        return "+" + lifeCycles.toString();
    }

    @Override
    public void install() {
        lifeCycles.forEach(LifeCycle::install);
    }
}
