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
package org.gradoop.examples.io.mag.magimport.gradoop;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.gradoop.examples.io.mag.magimport.InputSchema;
import org.gradoop.examples.io.mag.magimport.data.TableSchema;
import org.gradoop.examples.io.mag.magimport.parse.TableFileParser;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Main class, parsing mag csv data to Gradoop JSON files.
 *
 * @author p-f
 */
public class ImportMain {

    /**
     * Default maximum number of lines to parse.
     */
    private static final long PARSE_COUNT = 20000;

    /**
     * Logger of this class.
     */
    private static final Logger LOG = Logger
            .getLogger(ImportMain.class.getName());

    /**
     * Helper method for creating the graph from the
     * {@link GradoopElementProcessor processor} storing it to disk.
     *
     * @param source Source processor.
     * @param targetDir Output directory.
     */
    private static void createGraphFrom(GradoopElementProcessor source,
            Path targetDir) {
        ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();
        GradoopFlinkConfig cfg = GradoopFlinkConfig.createConfig(env);
        List<ImportVertex<String>> vertices = source.getResultVertices();
        List<ImportEdge<String>> edges = source.getResultEdges();
        LOG.info("Processing " + vertices.size() + " vertices and "
                + edges.size() + " edges.");
        DataSet<ImportVertex<String>> vDataSet = env.fromCollection(vertices);
        DataSet<ImportEdge<String>> eDataSet = env.fromCollection(edges);
        DataSource graphSource = new GraphDataSource(vDataSet, eDataSet, cfg);
        DataSink sink = new JSONDataSink(targetDir.toString(), cfg);
        String target = targetDir.toString() + "/";
        LogicalGraph graph;
        try {
            graph = graphSource.getLogicalGraph();
        }
        catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to create graph.", ex);
            return;
        }
        try {
            LOG.info("Writing graph");
            sink.write(graph, true);
        }
        catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to write graph.", ex);
        }
        try {
            env.execute();
        }
        catch (Exception ex) {
            LOG.finest("Hier könnte ihre Werbung stehen.");
            throw new RuntimeException("Execution failed.", ex);
        }

    }

    /**
     * Helper method to determine the parsing order of the input files.
     *
     * @param schema The schema to analyze.
     * @return An interger to compare later.
     */
    private static int getParseOrder(TableSchema schema) {
        switch (schema.getType()) {
            case EDGE:
                // Edges first.
                return 0;
            case EDGE_3:
                // Multiedges next.
                return 1;
            case NODE:
                // Nodes next.
                return 2;
            case MULTI_ATTRIBUTE:
                // Attributes next.
                return 3;
            default:
                // Every thing else.
                return 4;
        }
    }

    /**
     * Main method, reading the graph from disk, writing the result to disk.
     *
     * @param args Usage: INPUTPATH OUTPUTPATH [COUNT]
     */
    public static void main(String[] args) {
        System.err.println("Running with " + Arrays.toString(args));
        Path graphRoot = Paths.get(args.length == 0 ? "." : args[0]);
        if (!graphRoot.toFile().isDirectory()) {
            System.err.println("Graph root not found.");
            System.out.println("Usage: ImportMain INPATH OUTPATH");
            return;
        }
        Path outPath = Paths.get(args.length < 2 ? "." : args[1]);
        if (outPath.toFile().isFile()) {
            System.err.println("Output path is file.");
            System.out.println("Usage: ImportMain INPATH OUTPATH");
            return;
        } else if (!Files.exists(outPath)) {
            LOG.info("Creating output directory " + outPath.toString());
            outPath.toFile().mkdirs();
        }
        String rootDir = graphRoot.toString();
        
        final long maxParseCount = args.length >= 3 ?
                Long.parseLong(args[2]) : PARSE_COUNT;
        
        // Reverse the order to make sure papers are processed first.
        Map<String, TableSchema> files = InputSchema.get();

        GradoopElementProcessor processor
                = new GradoopElementProcessor(files.values());

        ExecutorService runner = Executors.newSingleThreadExecutor();

        // Submit each tast ordered by the getParseOrder helper method.
        files.entrySet().stream().sorted(Comparator
                .comparingInt(e -> getParseOrder(e.getValue())))
                .map(e -> new TableFileParser(e.getValue(),
                Paths.get(rootDir, e.getKey() + ".txt"), processor,
                maxParseCount))
                .forEach(runner::submit);

        runner.submit(() -> ImportMain
                .createGraphFrom(processor, outPath.toAbsolutePath()));
        runner.shutdown();
    }
}
