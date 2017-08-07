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
package org.gradoop.examples.io.mag.magimport.grouping;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import org.gradoop.examples.io.mag.magimport.grouping.aggregation.MapSumAggregator;
import org.gradoop.examples.io.mag.magimport.grouping.transformation.JoinAttributes;
import org.gradoop.examples.io.mag.magimport.grouping.transformation.SplitAttributes;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
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
     * @param args Usage: INPUTPATH OUTPUTPATH
     * @throws Exception I dont know ask the developer?
     */
    public static void main(final String[] args) throws Exception {
        // TODO: Improve argument handling.
        String inputPath = args[0];
        String outputPath = args[1];

        Path outPath = Paths.get(outputPath);
        if (outPath.toFile().isFile()) {
            System.err.println("Output path is file.");
            System.out.println("Usage: ImportMain INPATH OUTPATH");
            return;
        } else if (!outPath.toFile().exists()) {
            LOG.info("Creating output directory " + outPath.toString());
            outPath.toFile().mkdirs();
        }

        ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();

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

        // instantiate a data sink for the DOT format
        DataSink dataSink = new DOTDataSink(outputPath, false);
        dataSink.write(schema, true);

        // run the job
        env.execute();
    }
}