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

import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import one.p_f.testing.magimport.data.MagObject;
import one.p_f.testing.magimport.data.TableSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * A {@link FlatMapFunction} that turns an {@link MagObject} into a set of nodes
 * and vertices.
 */
public class MagObjectToGradoopMapper
        implements FlatMapFunction<MagObject, Tuple> {

    /**
     * Logger of this class.
     */
    private static final Logger LOG
            = Logger.getLogger(MagObjectToGradoopMapper.class.getName());

    @Override
    public void flatMap(MagObject value, Collector<Tuple> out) {
        switch (value.getSchema().getType()) {
            case NODE:
                Optional<String> id = getId(value);
                if (id.isPresent()) {
                    Properties prop = convertAttributes(value);
                    out.collect(new ImportVertex<>(id.get(),
                            value.getSchema().getSchemaName(), prop));
                } else {
                    LOG.log(Level.WARNING, "Dropping {0}. (No ID.)",
                            value.toString());
                }
                break;
            // TODO: implement other types.
        }
    }

    /**
     * Convert {@link MagObject}s attributes to {@link Properties}.
     *
     * @param obj Object to get attributes from.
     * @return {@link Properties} used in Gradoop.
     */
    private static Properties convertAttributes(MagObject obj) {
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
     * Get the id of an object if it has an id.
     *
     * @param obj The object.
     * @return The ID.
     */
    private static Optional<String> getId(MagObject obj) {
        // TODO: Cache index per TableSchema to reduce complexity.
        List<TableSchema.FieldType> types = obj.getSchema().getFieldTypes();
        for (int i = 0; i < types.size(); i++) {
            if (types.get(i).equals(TableSchema.FieldType.ID)) {
                return Optional.of(obj.getFieldData(i));
            }
        }
        return Optional.empty();
    }
}
