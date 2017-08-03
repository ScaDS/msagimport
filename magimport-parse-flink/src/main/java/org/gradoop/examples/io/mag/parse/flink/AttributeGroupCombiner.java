/*
 * Copyright 2017 Philip Fritzsche <p-f@users.noreply.github.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.examples.io.mag.parse.flink;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * A combiner that combines pairs of key and properties by key. This must only
 * be used on DataSets grouped by KEY. Also acts as a custom join to join
 * multiattributes with nodes.
 */
public class AttributeGroupCombiner implements
        GroupCombineFunction<Tuple2<String, Properties>, Tuple2<String, Properties>>,
        JoinFunction<ImportVertex<String>, Tuple2<String, Properties>, ImportVertex<String>> {

    /**
     * Combine 2 {@link Properties} to 1. Elements will be combined using
     * {@link #combine(PropertyValue, PropertyValue)}.
     *
     * @param first The first object.
     * @param second The second object.
     * @return The first object (with all values from the second added).
     */
    public static Properties combine(Properties first, Properties second) {
        for (Property prop : second) {
            if (first.containsKey(prop.getKey())) {
                PropertyValue firstVal = first.remove(prop.getKey());
                PropertyValue secondVal = prop.getValue();
                first.set(prop.getKey(), combine(firstVal, secondVal));
            } else {
                first.set(prop);
            }
        }
        return first;
    }

    /**
     * Combine 2 {@link PropertyValue}s to 1. The result will be a {@link List}
     * that is a union of the 2 input values. ({@link PropertyValue}s that are
     * not {@link List}s will be converted to lists first.
     *
     * @param first The first value.
     * @param second The second value.
     * @return The union of both values.
     */
    public static PropertyValue combine(PropertyValue first,
            PropertyValue second) {
        List<PropertyValue> values = first.isList() ? first.getList()
                : new ArrayList<>();
        if (second.isList()) {
            values.addAll(second.getList());
        } else {
            values.add(second);
        }
        return PropertyValue.create(values);
    }

    @Override
    public void combine(Iterable<Tuple2<String, Properties>> values,
            Collector<Tuple2<String, Properties>> out)
            throws MagParserException {
        String key = null;
        Properties prop = null;
        for (Tuple2<String, Properties> value : values) {
            if (key == null) {
                key = value.f0;
            } else if (!key.equals(value.f0)) {
                throw new MagParserException("Key mismatch.");
            }
            prop = prop == null ? value.f1 : combine(prop, value.f1);
        }
        out.collect(new Tuple2<>(key, prop));
    }

    @Override
    public ImportVertex<String> join(ImportVertex<String> first,
            Tuple2<String, Properties> second) throws MagParserException {
        return new ImportVertex<>(second.f0, first.f1,
                combine(first.f2, second.f1));
    }

}
