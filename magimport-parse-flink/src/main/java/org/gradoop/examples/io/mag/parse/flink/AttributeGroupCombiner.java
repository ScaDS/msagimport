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
import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * A combiner that combines pairs of key and properties by key. This must only
 * be used on DataSets grouped by KEY.
 */
public class AttributeGroupCombiner implements
        GroupCombineFunction<Tuple2<String, Properties>, Tuple2<String, Properties>> {

    /**
     * An operator that merges {@link PropertyValue}s.
     */
    private static class ValueMerger implements BinaryOperator<PropertyValue> {

        @Override
        public PropertyValue apply(PropertyValue t, PropertyValue u) {
            List<PropertyValue> values = t.isList() ? t.getList()
                    : new ArrayList<>();
            if (u.isList()) {
                values.addAll(u.getList());
            } else {
                values.add(u);
            }
            return PropertyValue.create(values);
        }

    }

    @Override
    public void combine(Iterable<Tuple2<String, Properties>> values,
            Collector<Tuple2<String, Properties>> out)
            throws MagParserException {
        Map<String, List<Tuple2<String, Properties>>> grouped
                = StreamSupport.stream(values.spliterator(), false)
                        .collect(Collectors.groupingBy(e -> e.f0));
        if (grouped.size() > 1) {
            throw new MagParserException("Non-unique key in grouped dataset.");
        } else if (grouped.isEmpty()) {
            return;
        }
        Map<String, PropertyValue> merged = grouped.values().iterator().next()
                .stream().map(e -> e.f1)
                .flatMap(e -> StreamSupport.stream(e.spliterator(), false))
                .collect(Collectors.groupingBy(Property::getKey, Collectors
                        .reducing(PropertyValue.create(new ArrayList<>()),
                                Property::getValue, new ValueMerger())));
    }

}
