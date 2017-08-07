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
import org.gradoop.examples.io.mag.magimport.data.MagObject;
import org.gradoop.examples.io.mag.magimport.data.TableSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.examples.io.mag.parse.flink.util.MagUtils;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * A flat map function mapping a {@link MagObject} to gradoop edges.
 */
public class Edge3FlatMapper implements FlatMapFunction<MagObject, ImportEdge<String>>,
        Serializable {

    @Override
    public void flatMap(MagObject value, Collector<ImportEdge<String>> out)
            throws MagParserException {
        String key = MagUtils.getByTypeSingle(value, TableSchema.FieldType.KEY)
                .orElseThrow(() -> new MagParserException(
                "Malformed edge3 (no KEY): " + value));
        String key1 = MagUtils.getByTypeSingle(value,
                TableSchema.FieldType.KEY_1).orElseThrow(()
                        -> new MagParserException(
                        "Malformed edge3 (no KEY_1): " + value));
        String key2 = MagUtils.getByTypeSingle(value,
                TableSchema.FieldType.KEY_2).orElseThrow(()
                        -> new MagParserException(
                        "Malformed edge3 (no KEY_2): " + value));
        Properties prop1 = MagUtils.convertAttributes(value);
        Properties prop2 = MagUtils.convertAttributes1(value);
        String schemaName = value.getSchema().getSchemaName();
        out.collect(makeEdge(key, key1, prop1, schemaName, 1));
        out.collect(makeEdge(key, key2, prop2, schemaName, 2));
    }

    /**
     * Helper method creating an edge.
     *
     * @param source Source node id.
     * @param target Target node id.
     * @param prop Properties of the edge.
     * @param schema Schema name of the edge.
     * @param run Number to append to label.
     * @return The edge.
     */
    private static ImportEdge<String> makeEdge(String source, String target,
            Properties prop, String schema, int run) {
        return new ImportEdge<>(source + '|' + target, source, target,
                schema + '_' + run, prop);
    }

}
