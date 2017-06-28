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
package one.p_f.testing.msagimport.gradoop;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.IntStream;
import one.p_f.testing.msagimport.callback.ElementProcessor;
import one.p_f.testing.msagimport.data.MsagObject;
import one.p_f.testing.msagimport.data.TableSchema;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 *
 * @author p-f
 */
public class GradoopElementProcessor implements ElementProcessor {

    private static final Logger LOG
            = Logger.getLogger(GradoopElementProcessor.class);

    private Map<String, TableSchema> graphSchema;

    public GradoopElementProcessor(Set<TableSchema> schemata) {
        graphSchema = new TreeMap<>();
        schemata.iterator().forEachRemaining(t
                -> graphSchema.put(t.getSchemaName(), t));
    }

    @Override
    public void process(MsagObject node) {
        switch (node.getSchema().getType()) {
            case NODE:
                Optional<String> id = getId(node);
                if (!id.isPresent()) {
                    LOG.warn("No id present on " + node.toString());
                    return;
                }
                Properties prop = convertAttributes(node);
                ImportVertex<String> vertex = new ImportVertex<>(id.get(),
                        node.getSchema().getSchemaName(), prop);
                // what now?
                break;

        }
    }

    /**
     * Get foreign keys of a {@link MsagObject}.
     *
     * @param obj Object to get keys from.
     * @return A map storing table and id of the foreign object.
     */
    private Map<TableSchema, String> getForeignKeys(MsagObject obj) {
        List<TableSchema.FieldType> types = obj.getSchema().getFieldTypes();
        List<String> fieldNames = obj.getSchema().getFieldNames();
        Map<TableSchema, String> keys = new TreeMap<>(Comparator
                .comparing(TableSchema::getSchemaName));
        for (int i = 1; i < types.size(); i++) {
            if (!types.get(i).equals(TableSchema.FieldType.KEY)) {
                continue;
            }
            String[] name = fieldNames.get(i)
                    .split(String.valueOf(TableSchema.SCOPE_SEPARATOR));
            if (name.length != 2) {
                LOG.warn("Malformed key column name: " + fieldNames.get(i));
                continue;
            }
            TableSchema targetSchema = graphSchema.get(name[0]);
            if (targetSchema == null) {
                LOG.warn("Foreign key to unknown table: " + name[0]);
                continue;
            }
            keys.put(targetSchema, obj.getFieldData(i));
        }
        return keys;
    }

    /**
     * Convert {@link MsagObject}s attributes to {@link Properties}.
     *
     * @param obj Object to get attributes from.
     * @return {@link Properties} used in Gradoop.
     */
    private static Properties convertAttributes(MsagObject obj) {
        Properties prop = new Properties();
        List<String> names = obj.getSchema().getFieldNames();
        List<TableSchema.FieldType> types = obj.getSchema().getFieldTypes();
        IntStream.range(0, types.size()).filter(i
                -> types.get(i).equals(TableSchema.FieldType.ATTRIBUTE))
                .forEach(i -> prop.set(names.get(i), obj.getFieldData(i)));
        return prop;
    }

    /**
     * Get the ID of an {@link MsagObject} if it has an ID.
     *
     * @param obj Object to get ID from.
     * @return ID (as {@link Optional}).
     */
    private static Optional<String> getId(MsagObject obj) {
        List<TableSchema.FieldType> types = obj.getSchema().getFieldTypes();
        for (int i = 0; i < types.size(); i++) {
            if (types.get(i).equals(TableSchema.FieldType.ID)) {
                return Optional.of(obj.getFieldData(i));
            }
        }
        return Optional.empty();
    }
}
