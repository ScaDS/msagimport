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
 * Aggregates corresponding values of maps.
 *
 * @author TraderJoe95 <johannes.leupold@schie-le.de>
 */
public class MapSumAggregator extends PropertyValueAggregator {

    /**
     * Class version for serialization.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Map to aggregate property keys in.
     */
    private final HashMap<PropertyValue, PropertyValue> aggregate;

    /**
     * Creates a new <code>MapSumAggregator</code>.
     *
     * @param propertyKey property key to access values
     * @param aggregatePropertyKey property key for final aggregate value
     */
    public MapSumAggregator(String propertyKey, String aggregatePropertyKey) {
        super(propertyKey, aggregatePropertyKey);
        aggregate = new HashMap<>();
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
                -> aggregate.put(a, addNumbers(
                        newAttributesMap.get(a),
                        aggregate.get(a))));
    }

    @Override
    protected PropertyValue getAggregateInternal() {
        return PropertyValue.create(aggregate);
    }

    @Override
    public void resetAggregate() {
        aggregate.clear();
    }

    /**
     * Adds PropertyValues of the same {@link Number} type.
     *
     * @param a first value of sum, <code>null</code> may be used as 0
     * @param b second velue of sum, <code>null</code> may be used as 0
     * @return <code>a + b</code> or <code>null</code> if both are
     * <code>null</code>
     */
    private PropertyValue addNumbers(PropertyValue a, PropertyValue b) {
        if (a == null && b == null) {
            return null;
        }
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        Number sum;
        if (a.isInt() && b.isInt()) {
            sum = a.getInt() + b.getInt();
        } else if (a.isLong() && b.isLong()) {
            sum = a.getLong() + b.getLong();
        } else if (a.isFloat() && b.isFloat()) {
            sum = a.getFloat() + b.getFloat();
        } else if (a.isDouble() && b.isDouble()) {
            sum = a.getDouble() + b.getDouble();
        } else if (a.isBigDecimal() && b.isBigDecimal()) {
            sum = a.getBigDecimal().add(b.getBigDecimal());
        } else {
            throw new IllegalArgumentException(
                    "Value types do not match or are not supported.");
        }

        return PropertyValue.create(sum);
    }
}
