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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.IntStream;
import one.p_f.testing.msagimport.callback.ElementProcessor;
import one.p_f.testing.msagimport.data.MsagObject;
import one.p_f.testing.msagimport.data.TableSchema;
import org.apache.log4j.Logger;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GConstants;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 *
 * @author p-f
 */
public class GradoopElementProcessor implements ElementProcessor {

    private static final Logger LOG
            = Logger.getLogger(GradoopElementProcessor.class);

    private Map<String, TableSchema> graphSchema;

    private final List<ImportVertex<String>> nodes;
    private final List<ImportEdge<String>> edges;

    public GradoopElementProcessor(Set<TableSchema> schemata) {
        graphSchema = new TreeMap<>();
        schemata.iterator().forEachRemaining(t
                -> graphSchema.put(t.getSchemaName(), t));
        nodes = new ArrayList<>(20000);
        edges = new ArrayList<>(20000);
    }

    @Override
    public void process(MsagObject obj) {
        Properties prop;
        switch (obj.getSchema().getType()) {
            case NODE:
                Optional<String> id = getId(obj);
                if (!id.isPresent()) {
                    LOG.warn("No id present on " + obj.toString());
                    return;
                }
                prop = convertAttributes(obj);
                ImportVertex<String> vertex = new ImportVertex<>(id.get(),
                        obj.getSchema().getSchemaName(), prop);

                nodes.add(vertex);
                getForeignKeys(obj).entrySet().stream()
                        .map(e -> new ImportEdge<String>(id.get() + '|'
                                + e.getValue(), id.get(), e.getValue()))
                        .forEach(edges::add);
                break;
            case EDGE:
                prop = convertAttributes(obj);
                Map<TableSchema, String> keys = getForeignKeys(obj);
                
                if (keys.size() != 2) {
                    LOG.warn("Malformed edge " + obj.toString());
                }
                
                Iterator<Entry<TableSchema, String>> it = keys.entrySet()
                        .iterator();
                String source = it.next().getValue();
                String target = it.next().getValue();
                
                ImportEdge<String> edge = new ImportEdge<>(source + '|' 
                        + target, source, target, GConstants.DEFAULT_EDGE_LABEL, 
                        prop);
                
                edges.add(edge);
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
        for (int i = 0; i < types.size(); i++) {
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
