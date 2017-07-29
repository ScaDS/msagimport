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

import java.util.HashMap;
import java.util.Map;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;

/**
 * Aggregates semicolon separated attribute lists using a Set.
 *
 * @author TraderJoe95 <johannes.leupold@schie-le.de>
 */
public class MapSumAggregator extends PropertyValueAggregator {

    /**
     * Map to aggregate property keys in.
     */
    private HashMap<PropertyValue, PropertyValue> attributeCount;

    /**
     * Creates a new <code>AttributeSetAggregator</code>.
     */
    public MapSumAggregator() {
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
        Map<PropertyValue, PropertyValue> newAttributesMap = in.getMap();
        newAttributesMap.keySet().stream().forEach((PropertyValue a)
                -> attributeCount.put(a, PropertyValue.create(
                        attributeCount
                        .getOrDefault(a, PropertyValue.create(0)).getInt()
                        + newAttributesMap
                        .getOrDefault(a, PropertyValue.create(0)).getInt())));
    }

    @Override
    protected PropertyValue getAggregateInternal() {
        return PropertyValue.create(attributeCount);
    }

    @Override
    public void resetAggregate() {
        attributeCount.clear();
    }

}
