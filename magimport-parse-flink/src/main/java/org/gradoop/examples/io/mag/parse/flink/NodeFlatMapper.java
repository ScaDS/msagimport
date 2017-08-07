/**
 * Copyright 2017 The magimport contributers.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.examples.io.mag.parse.flink.util.MagUtils;

/**
 * A flat map function mapping a {@link MagObject} to gradoop vertices and
 * edges. Both are mapped to tuple, because vertices and edges are tuples of
 * different sizes.
 */
public class NodeFlatMapper implements FlatMapFunction<MagObject, EdgeOrVertex<String>>,
        Serializable {

    /**
     * Delimiter to split the KEY fields by.
     */
    private String delimiter;

    /**
     * Column number of foreign keys.
     */
    private int[] foreignKeys;

    /**
     * Index of the id column;
     */
    private int idIndex;

    /**
     * Was this mapper initialized?
     */
    private boolean initialized;

    /**
     * Schema of objects to be mapped.
     */
    private TableSchema schema;

    /**
     * Create the mapper.
     */
    public NodeFlatMapper() {
        initialized = false;
    }

    @Override
    public void flatMap(MagObject value, Collector<EdgeOrVertex<String>> out)
            throws MagParserException {
        if (!initialized) {
            init(value);
        }
        // Compare by schema name only, as schema names must be unique.
        if (!value.getSchema().getSchemaName().equals(schema.getSchemaName())) {
            throw new MagParserException("Schema type mismatch: "
                    + value.toString());
        }
        String id = value.getFieldData(idIndex);
        Properties prop = MagUtils.convertAttributes(value);
        out.collect(new EdgeOrVertex(id, schema.getSchemaName(), prop));
        for (int keyIndex : foreignKeys) {
            String key = value.getFieldData(keyIndex);
            String schemaName = schema.getFieldNames().get(keyIndex)
                    .split(delimiter, -1)[0];
            out.collect(new EdgeOrVertex<>(id + '|' + key, id, key,
                    schema.getSchemaName() + '|' + schemaName,
                    Properties.create()));

        }
    }

    private void init(MagObject first) throws MagParserException {
        schema = first.getSchema();
        if (!schema.getType().equals(TableSchema.ObjectType.NODE)) {
            throw new MagParserException("Object is not a node: "
                    + first.toString());
        }
        List<TableSchema.FieldType> types = schema.getFieldTypes();
        delimiter = String.valueOf(TableSchema.SCOPE_SEPARATOR);
        int[] idIndexNew = IntStream.range(0, types.size())
                .filter(e -> types.get(e).equals(TableSchema.FieldType.ID))
                .toArray();
        foreignKeys = IntStream.range(0, types.size())
                .filter(e -> types.get(e).equals(TableSchema.FieldType.KEY))
                .toArray();
        if (idIndexNew.length != 1) {
            throw new MagParserException("Illegal number of IDs found: "
                    + first.toString());
        } else {
            idIndex = idIndexNew[0];
        }
        initialized = true;
    }

}
