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
import java.util.stream.IntStream;
import one.p_f.testing.magimport.data.MagObject;
import one.p_f.testing.magimport.data.TableSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.examples.io.mag.parse.flink.util.MagUtils;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * A flat map function mapping a {@link MagObject} to gradoop edges.
 */
public class Edge3FlatMapper implements FlatMapFunction<MagObject, ImportEdge<String>> {

    @Override
    public void flatMap(MagObject value, Collector<ImportEdge<String>> out)
            throws MagParserException {
        TableSchema schema = value.getSchema();
        List<TableSchema.FieldType> types = schema.getFieldTypes();
        int[] keys1 = IntStream.range(0, types.size())
                .filter(e -> types.get(e).equals(TableSchema.FieldType.KEY_1))
                .toArray();
        int[] keys2 = IntStream.range(0, types.size())
                .filter(e -> types.get(e).equals(TableSchema.FieldType.KEY_2))
                .toArray();
        if (keys1.length != 2 || keys2.length != 2) {
            throw new MagParserException("Illegal number of foreign keys: "
                    + value.toString());
        }
        Properties prop1 = MagUtils.convertAttributes(value);
        Properties prop2 = MagUtils.convertAttributes1(value);
        out.collect(makeEdge(keys1, prop1, value, 1));
        out.collect(makeEdge(keys2, prop2, value, 2));
    }

    /**
     * Helper method creating an edge.
     *
     * @param key Index for key columns.
     * @param prop Properties of the edge.
     * @param obj The object.
     * @param run Number to append to label.
     * @return The edge.
     */
    private static ImportEdge<String> makeEdge(int[] key, Properties prop,
            MagObject obj, int run) {
        String source = obj.getFieldData(key[0]);
        String target = obj.getFieldData(key[1]);
        return new ImportEdge<>(source + '|' + target, source, target,
                obj.getSchema().getSchemaName() + '_' + run, prop);
    }

}
