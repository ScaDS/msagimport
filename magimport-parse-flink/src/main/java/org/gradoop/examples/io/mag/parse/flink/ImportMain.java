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

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import one.p_f.testing.magimport.InputSchema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Main class for importing mag data via flink.
 */
public class ImportMain {

    /**
     * Logger of this class.
     */
    private static final Logger LOG
            = Logger.getLogger(ImportMain.class.getName());

    /**
     * @param args Usage: -i INPATH -o OUTPATH
     */
    public static void main(String[] args) {
        Options cliOptions = new Options();
        cliOptions.addOption("h", "help", false, "Show this help.");
        cliOptions.addRequiredOption("i", "input", true, "Input path.");
        cliOptions.addRequiredOption("o", "output", true, "Output path.");
        CommandLine cliConfig;
        try {
            cliConfig = new DefaultParser().parse(cliOptions, args);
        } catch (ParseException ex) {
            LOG.log(Level.SEVERE, "Failed to parse command line options: {0}",
                    ex.getMessage());
            showHelp(cliOptions);
            return;
        }
        if (cliConfig.hasOption('h')) {
            showHelp(cliOptions);
            return;
        }
        String inPath = cliConfig.getOptionValue('i');
        Path outPath = new Path(cliConfig.getOptionValue('o'));
        ExecutionEnvironment localEnv = ExecutionEnvironment
                .createLocalEnvironment();
        FlinkParser parser
                = new FlinkParser(inPath, localEnv, InputSchema.get());
        DataSet<ImportVertex<String>> vertices;
        DataSet<ImportEdge<String>> edges;
        try {
            vertices = parser.getVertices();
            edges = parser.getEdges();
        } catch (MagParserException mpe) {
            LOG.log(Level.SEVERE, "Parsing failed.", mpe);
            return;
        }
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(localEnv);
        DataSource source = new GraphDataSource(vertices, edges, config);
        LogicalGraph graph;
        try {
            graph = source.getLogicalGraph();
        } catch (IOException ioe) {
            LOG.log(Level.SEVERE, "Failed to create logical graph.", ioe);
            return;
        }
        DataSink sink = new JSONDataSink(outPath.toString(), config);
        try {
            sink.write(graph, true);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to write graph.", ex);
            return;
        }
        try {
            localEnv.execute("ImportGraph");
        } catch (Exception ex) {
            LOG.log(Level.SEVERE, "Execution failed.", ex);
            throw new RuntimeException(ex);
        }
    }

    /**
     * Helper method showing help.
     *
     * @param o Options to generate help from.
     */
    private static void showHelp(Options o) {
        new HelpFormatter().printHelp("java ImportMain", o, true);
    }

}
