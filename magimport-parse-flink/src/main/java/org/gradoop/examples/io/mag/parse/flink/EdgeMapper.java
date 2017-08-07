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

import java.io.Serializable;
import java.util.List;
import java.util.stream.IntStream;
import org.gradoop.examples.io.mag.magimport.data.MagObject;
import org.gradoop.examples.io.mag.magimport.data.TableSchema;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.examples.io.mag.parse.flink.util.MagUtils;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * A mapper mapping a {@link MagObject} to a gradoop edge.
 */
public class EdgeMapper implements MapFunction<MagObject, ImportEdge<String>>,
        Serializable {

    /**
     * Delimiter to split the KEY fields by.
     */
    private String delimiter;

    /**
     * Was this mapper initialized?
     */
    private boolean initialized;

    /**
     * Schema of objects to be mapped.
     */
    private TableSchema schema;

    /**
     * The column number of the first foreign key column.
     */
    private int sourceIndex;

    /**
     * The column number of the second foreign key column.
     */
    private int targetIndex;

    /**
     * Create the mapper.
     */
    public EdgeMapper() {
        initialized = false;
    }

    @Override
    public ImportEdge<String> map(MagObject value) throws MagParserException {
        if (!initialized) {
            init(value);
        }
        // Compare by schema name only, as schema names must be unique.
        if (!value.getSchema().getSchemaName().equals(schema.getSchemaName())) {
            throw new MagParserException("Schema type mismatch: "
                    + value.toString());
        }
        Properties prop = MagUtils.convertAttributes(value);
        String source = value.getFieldData(sourceIndex);
        String target = value.getFieldData(targetIndex);
        return new ImportEdge<>(source + '|' + target, source, target,
                schema.getSchemaName(), prop);
    }

    /**
     * Initialize the mapper. Should be called with the first element.
     *
     * @param first First object.
     * @throws MagParserException If the object is not a valid EDGE.
     */
    private void init(MagObject first) throws MagParserException {
        if (!first.getSchema().getType().equals(TableSchema.ObjectType.EDGE)) {
            throw new MagParserException("Object is not an edge: "
                    + first.toString());
        }
        List<TableSchema.FieldType> types = first.getSchema().getFieldTypes();
        int[] index = IntStream.range(0, types.size()).sequential()
                .filter(e -> types.get(e).equals(TableSchema.FieldType.KEY))
                .toArray();
        if (index.length != 2) {
            throw new MagParserException("Illegal number of foreign key on "
                    + first.toString() + " (" + index.length + ")");
        }
        schema = first.getSchema();
        sourceIndex = index[0];
        targetIndex = index[1];
        delimiter = String.valueOf(TableSchema.SCOPE_SEPARATOR);
        initialized = true;
    }

}
