/**
 * Copyright 2017 The magimport contributers.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.examples.io.mag.magimport.grouping;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.*;
import org.gradoop.examples.io.mag.magimport.grouping.aggregation.MapSumAggregator;
import org.gradoop.examples.io.mag.magimport.grouping.transformation.JoinAttributes;
import org.gradoop.examples.io.mag.magimport.grouping.transformation.SplitAttributes;
import org.gradoop.flink.io.api.DataSource;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.PropertyValueAggregator;
import org.gradoop.flink.model.impl.operators.grouping.functions.aggregation.CountAggregator;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Main class grouping the graph.
 */
public class GroupingMain {

    /**
     * Logger of this class.
     */
    private static final Logger LOG = Logger
            .getLogger(GroupingMain.class.getName());

    /**
     * Main method, reading the graph, grouping it, writing the result to disk.
     *
     * @param args Use {@code --help} to check usage.
     * @throws Exception If the execution fails.
     */
    public static void main(final String[] args) throws Exception {
        Options cliOptions = new Options();
        cliOptions.addOption("h", "help", false, "Show this help.");
        cliOptions.addRequiredOption("i", "input", true, "Input path.");
        cliOptions.addRequiredOption("o", "output", true, "Output path.");
        cliOptions.addOption("d", "dot-output", true, "Output dot file.");
        cliOptions.addOption("l", "force-local", false,
                "Enforce the use of local ExecutionEnvironment");
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
        String inputPath = cliConfig.getOptionValue('i');
        String outPath = cliConfig.getOptionValue('o');
        ExecutionEnvironment env = cliConfig.hasOption('l') ? ExecutionEnvironment.createLocalEnvironment() :
                ExecutionEnvironment.getExecutionEnvironment();

        // instantiate a default gradoop config
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        // define a data source to load the graph
        DataSource dataSource = new JSONDataSource(inputPath, config);

        // load the graph
        LogicalGraph graph = dataSource.getLogicalGraph();

        // transform attributes to attributes maps
        JoinAttributes joiner = new JoinAttributes("attributes");
        graph = joiner.execute(graph);

        // use graph grouping to extract the schema
        List<PropertyValueAggregator> vertexAgg = Arrays
                .asList(new MapSumAggregator("attributes", "attributesAgg"),
                        new CountAggregator());
        List<PropertyValueAggregator> edgeAgg = Arrays
                .asList(new MapSumAggregator("attributes", "attributesAgg"),
                        new CountAggregator());
        LogicalGraph schema = graph.groupBy(
                Collections.singletonList(Grouping.LABEL_SYMBOL),
                vertexAgg,
                Collections.singletonList(Grouping.LABEL_SYMBOL),
                edgeAgg,
                GroupingStrategy.GROUP_COMBINE);

        // transform aggregated attributes map to single attributes
        SplitAttributes splitter = new SplitAttributes("attributesAgg");
        schema = splitter.execute(schema);

        schema.writeTo(new JSONDataSink(outPath, config));
        if (cliConfig.hasOption('d')) {
            schema.writeTo(new DOTDataSink(cliConfig.getOptionValue('d'), false));
        }

        // run the job
        env.execute("Graph Grouping (Schema Graph Extraction)");
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
