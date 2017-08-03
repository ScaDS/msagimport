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
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;
import one.p_f.testing.magimport.data.MagObject;
import one.p_f.testing.magimport.data.TableSchema;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.examples.io.mag.parse.flink.util.MagUtils;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Parse a MAG dump using flink.
 */
public class FlinkParser {

    /**
     * Was the data parsed already?
     */
    private boolean parsed;

    /**
     * Environment to execute operations on.
     */
    private final ExecutionEnvironment environment;

    /**
     * Path of the graph root.
     */
    private final Path rootPath;

    /**
     * Schema of the graph to parse.
     */
    private final Map<String, TableSchema> graphSchema;

    /**
     * A map of all attributes assigned by table name.
     */
    private final Map<String, DataSet<Tuple2<String, Properties>>> attributes;

    /**
     * A map of all datasets of edges assigned by table name.
     */
    private final Map<String, DataSet<ImportEdge<String>>> edgeSets;

    /**
     * A map of all datasets of vertices assigned by table name.
     */
    private final Map<String, DataSet<ImportVertex<String>>> vertexSets;

    /**
     * A map of all datasets of vertices and edges assigned by table name.
     */
    private final Map<String, DataSet<EdgeOrVertex<String>>> combinedSets;

    /**
     * Create the parser.
     *
     * @param inPath Path to read the files from.
     * @param env Environment to execute the parser on.
     * @param graphSchema Schema of the input tsv data.
     */
    public FlinkParser(String inPath, ExecutionEnvironment env,
            Map<String, TableSchema> graphSchema) {
        parsed = false;
        environment = env;
        rootPath = new Path(inPath);
        this.graphSchema = graphSchema;
        attributes = new TreeMap<>();
        edgeSets = new TreeMap<>();
        vertexSets = new TreeMap<>();
        combinedSets = new TreeMap<>();
    }

    /**
     * Get a DataSet of all parsed edges.
     *
     * @return The DataSet.
     * @throws MagParserException if parsing or merging DataSets failed.
     */
    public DataSet<ImportEdge<String>> getEdges() throws MagParserException {
        parseAll();
        return edgeSets.values().stream().reduce(DataSet::union)
                .orElse(environment.fromElements());
    }

    /**
     * Get a DataSet of all parsed vertices.
     *
     * @return The DataSet.
     * @throws MagParserException if parsing or merging DataSets failed.
     */
    public DataSet<ImportVertex<String>> getVertices()
            throws MagParserException {
        parseAll();
        return vertexSets.values().stream().reduce(DataSet::union)
                .orElse(environment.fromElements());
    }

    /**
     * Parse a table and create a {@link DataSet} of {@link MagObject}s.
     *
     * @param table Table to parse.
     * @param schema Schema of the table.
     * @return The dataset.
     */
    private DataSet<MagObject> createFromInput(String table,
            TableSchema schema) {
        Path tablePath = new Path(rootPath, table + ".txt");
        return environment.createInput(new TextInputFormat(tablePath))
                .map(line -> new MagObject(schema)
                .setFieldData(line.split("\\t")));
    }

    /**
     * Parse all tables.
     */
    private void parseAll() throws MagParserException {
        if (parsed) {
            return;
        }
        // First parse all.
        for (String tableName : graphSchema.keySet()) {
            TableSchema schema = graphSchema.get(tableName);
            switch (schema.getType()) {
                case NODE:
                    combinedSets.put(tableName, parseNodes(tableName, schema));
                    break;
                case EDGE:
                    edgeSets.put(tableName, parseEdges(tableName, schema));
                    break;
                case EDGE_3:
                    edgeSets.put(tableName, parseEdges3(tableName, schema));
                    break;
                case MULTI_ATTRIBUTE:
                    attributes.put(tableName,
                            parseMultiAttributes(tableName, schema));
                    break;
                default:
                    throw new MagParserException("Unknown table type:"
                            + schema.getType());
            }
        }
        // Combine datasets.
        // Step 1: Split combined datasets.
        for (String name : combinedSets.keySet()) {
            DataSet<ImportVertex<String>> vertices = null;
            DataSet<ImportEdge<String>> edges = null;
            if (edgeSets.containsKey(name)) {
                edges = edgeSets.remove(name);
            }
            if (vertexSets.containsKey(name)) {
                vertices = vertexSets.remove(name);
            }
            DataSet<ImportVertex<String>> newVertices = combinedSets.get(name)
                    .filter(new FilterFunction<EdgeOrVertex<String>>() {
                        @Override
                        public boolean filter(EdgeOrVertex<String> value) {
                            return value.f1 == null;
                        }
                    })
                    .map(new EdgeOrVertex.ToVertex<>());
            vertexSets.put(name, vertices == null ? newVertices
                    : newVertices.union(vertices));
            DataSet<ImportEdge<String>> newEdges = combinedSets.get(name)
                    .filter(new FilterFunction<EdgeOrVertex<String>>() {
                        @Override
                        public boolean filter(EdgeOrVertex<String> value) {
                            return value.f1 != null;
                        }
                    })
                    .map(new EdgeOrVertex.ToEdge<>());
            edgeSets.put(name, edges == null ? newEdges
                    : newEdges.union(edges));
        }
        // Step 2: Add multiattributes.
        for (String name : attributes.keySet()) {
            // Find correct table.
            List<TableSchema.FieldType> types = graphSchema.get(name)
                    .getFieldTypes();
            String[] targetTable = IntStream.range(0, types.size())
                    .filter(e -> types.get(e).equals(TableSchema.FieldType.KEY))
                    .mapToObj(e -> graphSchema.get(name).getFieldNames().get(e))
                    .map(e -> e
                    .split(String.valueOf(TableSchema.SCOPE_SEPARATOR)))
                    .filter(e -> e.length == 2).map(e -> e[0])
                    .toArray(String[]::new);
            if (targetTable.length != 1) {
                throw new MagParserException(
                        "Illegal number of foreign keys for " + name);
            }
            DataSet<ImportVertex<String>> vertices = vertexSets.get(name);
            if (vertices == null) {
                throw new MagParserException(
                        "Attribute table with no matching nodes: " + name);
            }
            DataSet<ImportVertex<String>> joined = vertices
                    .join(attributes.get(name)).where(0).equalTo(0)
                    .with(new AttributeGroupCombiner());
            vertexSets.put(name, joined);
            attributes.remove(name);
        }
        parsed = true;
    }

    /**
     * Parse an edge table.
     *
     * @param tableName Table to parse.
     * @param schema Schema of the input data.
     * @return A dataset of edges.
     */
    private DataSet<ImportEdge<String>> parseEdges(String tableName,
            TableSchema schema) {
        String source = tableName;
        return createFromInput(tableName, schema)
                .map(new EdgeMapper());
    }

    /**
     * Parse an edge3 table.
     *
     * @param tableName Table to parse.
     * @param schema Schema of the input data.
     * @return A dataset of edges.
     */
    private DataSet<ImportEdge<String>> parseEdges3(String tableName,
            TableSchema schema) {
        return createFromInput(tableName, schema)
                .flatMap(new Edge3FlatMapper());
    }

    /**
     * Parse a multi attribute table.
     *
     * @param tableName Table to parse.
     * @param schema Schema of the input data.
     * @return A dataset with a foreign key to properties mapping.
     */
    private DataSet<Tuple2<String, Properties>> parseMultiAttributes(
            String tableName, TableSchema schema) {
        return createFromInput(tableName, schema)
                .map(new MapFunction<MagObject, Tuple2<String, Properties>>() {
                    @Override
                    public Tuple2<String, Properties> map(MagObject e) {
                        return new Tuple2<>(MagUtils
                                .getByTypeSingle(e, TableSchema.FieldType.KEY)
                                .orElse(null),
                                MagUtils.convertAttributes(e));
                    }
                })
                .filter(e -> e.f0 != null)
                .groupBy(0)
                .combineGroup(new AttributeGroupCombiner());
    }

    /**
     * Parse a node table.
     *
     * @param tableName Table to parse.
     * @param schema Schema of the input data.
     * @return A dataset of edges and vertices.
     */
    private DataSet<EdgeOrVertex<String>> parseNodes(String tableName,
            TableSchema schema) {
        return createFromInput(tableName, schema)
                .flatMap(new NodeFlatMapper());
    }
}
