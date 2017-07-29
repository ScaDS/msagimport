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
        attributeCount = new HashMap<>();
    }

    @Override
    protected boolean isInitialized() {
        return true;
    }

    @Override
    protected void initializeAggregate(PropertyValue in) {
    }

    @Override
    protected void aggregateInternal(PropertyValue in) {
        String str = in.getString();
        Arrays.stream(str.split(";")).forEach(a -> attributeCount
                .put(a, attributeCount.getOrDefault(a, 0) + 1));
    }

    @Override
    protected PropertyValue getAggregateInternal() {
        return PropertyValue.create(attributeCount.keySet().stream()
                .map(a -> a + "@" + attributeCount.get(a))
                .collect(Collectors.joining(";")));
    }

    @Override
    public void resetAggregate() {
        attributeCount.clear();
    }

}
