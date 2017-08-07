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
package org.gradoop.examples.io.mag.parse.flink.util;

import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import org.gradoop.examples.io.mag.magimport.data.MagObject;
import org.gradoop.examples.io.mag.magimport.data.TableSchema;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * Utilities for extracting information from {@link MagObject}s.
 */
public final class MagUtils {

    /**
     * No instances of this class are needed.
     */
    private MagUtils() {
    }

    /**
     * Convert {@link MagObject}s attributes to {@link Properties}.
     *
     * @param obj Object to get attributes from.
     * @return {@link Properties} used in Gradoop.
     */
    public static Properties convertAttributes(MagObject obj) {
        Properties prop = new Properties();
        List<String> names = obj.getSchema().getFieldNames();
        List<TableSchema.FieldType> types = obj.getSchema().getFieldTypes();
        IntStream.range(0, types.size()).filter(i
                -> types.get(i).equals(TableSchema.FieldType.ATTRIBUTE))
                .filter(i -> !obj.getFieldData(i).equals(""))
                .forEach(i -> prop.set(names.get(i), obj.getFieldData(i)));
        return prop;
    }

    /**
     * Same as {@link #convertAttributes(MagObject)}, except for ATTRIBUTE_1
     * instead of ATTRIBUTE.
     *
     * @param obj Object to get attributes from.
     * @return {@link Properties} used in Gradoop.
     */
    public static Properties convertAttributes1(MagObject obj) {
        Properties prop = new Properties();
        List<String> names = obj.getSchema().getFieldNames();
        List<TableSchema.FieldType> types = obj.getSchema().getFieldTypes();
        IntStream.range(0, types.size()).filter(i
                -> types.get(i).equals(TableSchema.FieldType.ATTRIBUTE_1))
                .filter(i -> !obj.getFieldData(i).equals(""))
                .forEach(i -> prop.set(names.get(i), obj.getFieldData(i)));
        return prop;
    }

    /**
     * Get a column from a {@link MagObject} that has a certain type. Returns an
     * empty {@link Optional} if more than 1 or 0 columns have the same type.
     *
     * @param source The object to extract from.
     * @param type Type of column to select.
     * @return An {@link Optional} containing the element or nothing.
     */
    public static Optional<String> getByTypeSingle(MagObject source,
            TableSchema.FieldType type) {
        List<TableSchema.FieldType> types = source.getSchema().getFieldTypes();
        int[] index = IntStream.range(0, types.size())
                .filter(e -> types.get(e).equals(type)).toArray();
        return index.length == 1 ? Optional.of(source.getFieldData(index[0]))
                : Optional.empty();
    }

    /**
     * Get the ID of an object if it has any.
     *
     * @param obj Object to get the ID of.
     * @return The ID as an {@link Optional}.
     */
    public static Optional<String> getId(MagObject obj) {
        List<TableSchema.FieldType> types = obj.getSchema().getFieldTypes();
        for (int i = 0; i < types.size(); i++) {
            if (types.get(i).equals(TableSchema.FieldType.ID)) {
                return Optional.of(obj.getFieldData(i));
            }
        }
        return Optional.empty();
    }
}
