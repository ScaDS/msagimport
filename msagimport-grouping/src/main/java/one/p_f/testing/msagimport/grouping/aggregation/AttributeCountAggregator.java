/*
 * Copyright 2017 TraderJoe95 <johannes.leupold@schie-le.de>.
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
package one.p_f.testing.msagimport.grouping.aggregation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Collectors;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

/**
 * Aggregates semicolon separated attribute lists using a Set.
 *
 * @author TraderJoe95 <johannes.leupold@schie-le.de>
 */
public class AttributeCountAggregator extends PropertyValueAggregator {

    /**
     * Map to aggregate property keys in.
     */
    private HashMap<String, Integer> attributeCount;

    /**
     * Creates a new <code>AttributeSetAggregator</code>.
     */
    public AttributeCountAggregator() {
        super("attributes", "attributes");
    }

    @Override
    protected boolean isInitialized() {
        return attributeCount != null && !attributeCount.isEmpty();
    }

    @Override
    protected void initializeAggregate(PropertyValue in) {
        String str = in.getString();
        attributeCount = new HashMap<>();
        Arrays.asList(str.split(";")).stream()
                .forEach(a -> attributeCount.put(a, 1));
    }

    @Override
    protected void aggregateInternal(PropertyValue in) {
        String str = in.getString();
        Arrays.asList(str.split(";")).stream().forEach(a -> attributeCount
                .put(a, attributeCount.getOrDefault(a, 0) + 1));
    }

    @Override
    protected PropertyValue getAggregateInternal() {
        HashSet<String> returnAttributeSet = new HashSet<>();
        attributeCount.keySet().stream().forEach(a -> returnAttributeSet
                .add(a + "@" + attributeCount.get(a)));
        return PropertyValue.create(returnAttributeSet.stream()
                .collect(Collectors.joining(";")));
    }

    @Override
    public void resetAggregate() {
        if (isInitialized()) {
            attributeCount.clear();
        }
    }

}
