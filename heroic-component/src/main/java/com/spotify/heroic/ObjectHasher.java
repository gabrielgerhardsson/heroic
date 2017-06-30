/*
 * Copyright (c) 2017 Spotify AB.
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

import com.google.common.hash.Hasher;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import lombok.Data;

@Data
public class ObjectHasher {
    private static final int STRING_FIELD = 1;
    private static final int OBJECT_FIELD = 2;
    private static final int LONG_FIELD = 3;
    private static final int LIST_FIELD = 4;
    private static final int DOUBLE_FIELD = 5;
    private static final int BOOLEAN_FIELD = 6;
    private static final int OPTIONAL_FIELD = 7;
    private static final int SET_FIELD = 8;

    private static final int START_OBJECT = 10;
    private static final int END_OBJECT = 11;
    private static final int START_LIST = 12;
    private static final int END_LIST = 13;
    private static final int START_SET = 14;
    private static final int END_SET = 15;

    private static final int OPTIONAL_PRESENT = 50;
    private static final int OPTIONAL_ABSENT = 51;

    private static final int STRING = 80;
    private static final int ENUM = 20;

    private final Hasher hasher;

    public void putStringField(final String name, final String value) {
        hasher.putInt(STRING_FIELD);
        putString(name);
        putString(value);
    }

    public <T> void putField(
        final String name, final T value, final BiConsumer<T, ObjectHasher> hashTo
    ) {
        hasher.putInt(OBJECT_FIELD);
        putString(name);
        hashTo.accept(value, this);
    }

    public <T> void putListField(
        final String name, final List<T> value, final BiConsumer<T, ObjectHasher> hashTo
    ) {
        hasher.putInt(LIST_FIELD);
        putString(name);
        hasher.putInt(value.size());
        putList(value, v -> hashTo.accept(v, this));
    }

    public <T> void putList(final List<T> value, final Consumer<T> hashTo) {
        hasher.putInt(START_LIST);

        for (final T val : value) {
            hashTo.accept(val);
        }

        hasher.putInt(END_LIST);
    }

    public <T> void putSortedSet(final SortedSet<T> value, final Consumer<T> hashTo) {
        hasher.putInt(START_SET);

        for (final T val : value) {
            hashTo.accept(val);
        }

        hasher.putInt(END_SET);
    }

    public void putObject(final Class<?> cls, final Consumer<ObjectHasher> consumer) {
        startObject(cls);
        consumer.accept(this);
        endObject();
    }

    private void startObject(final Class<?> cls) {
        hasher.putInt(START_OBJECT);
        putString(cls.getCanonicalName());
    }

    private void endObject() {
        hasher.putInt(END_OBJECT);
    }

    public void putString(final String name) {
        hasher.putInt(STRING);
        hasher.putInt(name.length());
        hasher.putString(name, StandardCharsets.UTF_8);
    }

    public void putLongField(final String name, final long value) {
        hasher.putInt(LONG_FIELD);
        putString(name);
        hasher.putLong(value);
    }

    public void putDoubleField(final String name, final double value) {
        hasher.putInt(DOUBLE_FIELD);
        putString(name);
        hasher.putDouble(value);
    }

    public void putBooleanField(final String name, final boolean value) {
        hasher.putInt(BOOLEAN_FIELD);
        putString(name);
        hasher.putBoolean(value);
    }

    public <T> void putOptionalField(
        final String name, final Optional<T> optional, final BiConsumer<T, ObjectHasher> hashTo
    ) {
        hasher.putInt(OPTIONAL_FIELD);
        putString(name);

        if (optional.isPresent()) {
            hasher.putInt(OPTIONAL_PRESENT);
            hashTo.accept(optional.get(), this);
        } else {
            hasher.putInt(OPTIONAL_ABSENT);
        }
    }

    public <T extends Enum> void putEnum(final T value) {
        hasher.putInt(ENUM);
        putString(value.getClass().getCanonicalName());
        putString(value.name());
    }
}
