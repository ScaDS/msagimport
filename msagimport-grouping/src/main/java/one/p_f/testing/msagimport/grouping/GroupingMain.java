/*
 * Copyright 2017 Johannes Leupold.
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
package one.p_f.testing.msagimport.grouping;

import java.util.Collections;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.util.GradoopFlinkConfig;


/**
 * @author TraderJoe95
 */
public class GroupingMain {

    public static void main(final String[] args) throws Exception {
        String inputPath = args[0];
        String outputPath = args[1];

        ExecutionEnvironment env = ExecutionEnvironment
                .getExecutionEnvironment();

        // instantiate a default gradoop config
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        // define a data source to load the graph
        DataSource dataSource = new JSONDataSource(inputPath, config);

        // load the graph
        LogicalGraph graph = dataSource.getLogicalGraph();

        // use graph grouping to extract the schema
        LogicalGraph schema = graph.groupBy(
                Collections.singletonList(Grouping.LABEL_SYMBOL),
                Collections.singletonList(Grouping.LABEL_SYMBOL));

        // instantiate a data sink for the DOT format
        DataSink dataSink = new DOTDataSink(outputPath, false);
        dataSink.write(schema, true);

        // run the job
        env.execute();
    }
}
