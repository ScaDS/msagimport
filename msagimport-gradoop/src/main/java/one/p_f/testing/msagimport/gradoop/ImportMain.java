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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import one.p_f.testing.msagimport.data.TableSchema;
import one.p_f.testing.msagimport.parse.TableFileParser;
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
 *
 * @author p-f
 */
public class ImportMain {

    private static final long PARSE_COUNT = 20000;

    private static final Logger LOG = Logger
            .getLogger(ImportMain.class.getName());

    public static void createGraphFrom(GradoopElementProcessor source,
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
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to create graph.", ex);
            return;
        }
        try {
            LOG.info("Writing graph");
            sink.write(graph, true);
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to write graph.", ex);
        }
        try {
            env.execute();
        } catch (Exception ex) {
            LOG.finest("Hier könnte ihre Werbung stehen.");
            throw new RuntimeException("Execution failed.", ex);
        }

    }

    public static void main(String[] args) {
        System.err.println("Running with " + Arrays.toString(args));
        Path graphRoot = Paths.get(args.length == 0 ? "." : args[0]);
        if (!graphRoot.toFile().isDirectory()) {
            System.err.println("Graph root not found.");
            System.out.println("Usage: ImportMain INPATH OUTPATH");
            return;
        }
        Path outPath = Paths.get(args.length < 2 ? "." : args[1]);
        if (!outPath.toFile().isDirectory()) {
            System.err.println("Output path not found.");
            System.out.println("Usage: ImportMain INPATH OUTPATH");
            return;
        }
        String rootDir = graphRoot.toString();

        // Reverse the order to make sure papers are processed first.
        Map<String, TableSchema> files
                = new TreeMap<>((first, second) -> second.compareTo(first));

        TableSchema schema = new TableSchema.Builder()
                .setSchemaName("Authors")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Author ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Author name")
                .build();
        files.put("Authors", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("Affiliations")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Affiliation ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Affiliation name")
                .build();
        files.put("Affiliations", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("ConferenceSeries")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Conference series ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Short name")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Full name")
                .build();
        files.put("Conferences", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("ConferenceInstances")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.KEY,
                        "ConferenceSeries:Conference series ID")
                .addField(TableSchema.FieldType.ID, "Conference instance ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Short name")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Full name")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Location")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Official conference URL")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Conference start date")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Conference end date")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Conference abstract registration date")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Conference submission deadline date")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Conference notification due date")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Conference final version due date")
                .build();
        files.put("ConferenceInstances", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("FieldsOfStudy")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Field of study ID")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Field of study name")
                .build();
        files.put("FieldsOfStudy", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("FieldOfStudyHierarchy")
                .setObjectType(TableSchema.ObjectType.EDGE)
                .addField(TableSchema.FieldType.KEY,
                        "FieldsOfStudy:Child field of study ID")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Child field of study level")
                .addField(TableSchema.FieldType.KEY,
                        "FieldsOfStudy:Parent field of stufy ID")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Parent field of study level")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Confidence")
                .build();
        files.put("FieldOfStudyHierarchy", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("Journals")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Journal ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Journal name")
                .build();
        files.put("Journals", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("Papers")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Paper ID")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Original paper title")
                .addField(TableSchema.FieldType.IGNORE,
                        "Normalized paper title")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Paper publish year")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Paper publish date")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "Paper Document Object Identifier")
                .addField(TableSchema.FieldType.IGNORE,
                        "Original venue name")
                .addField(TableSchema.FieldType.IGNORE,
                        "Normalized venue name")
                .addField(TableSchema.FieldType.KEY, "Journals:Journal ID")
                .addField(TableSchema.FieldType.KEY,
                        "ConferenceSeries:Conference series ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Paper rank")
                .build();
        files.put("Papers", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperAuthorAffiliations")
                .setObjectType(TableSchema.ObjectType.EDGE_3)
                .addField(TableSchema.FieldType.KEY_1, "Papers:Paper ID")
                .addField(TableSchema.FieldType.KEY, "Authors:Author ID")
                .addField(TableSchema.FieldType.KEY_2,
                        "Affiliations:Affiliation ID")
                .addField(TableSchema.FieldType.IGNORE,
                        "Original affiliation name")
                .addField(TableSchema.FieldType.IGNORE,
                        "Normalized affiliation name")
                .addField(TableSchema.FieldType.ATTRIBUTE_1,
                        "Author sequence number")
                .build();
        files.put("PaperAuthorAffiliations", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperKeywords")
                .setObjectType(TableSchema.ObjectType.EDGE)
                .addField(TableSchema.FieldType.KEY, "Papers:Paper ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Keyword name")
                .addField(TableSchema.FieldType.KEY,
                        "FieldsOfStudy:Field of study ID")
                .build();
        files.put("PaperKeywords", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperReferences")
                .setObjectType(TableSchema.ObjectType.EDGE)
                .addField(TableSchema.FieldType.KEY, "Papers:Paper ID")
                .addField(TableSchema.FieldType.KEY,
                        "Papers:Paper reference ID")
                .build();
        files.put("PaperReferences", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperUrls")
                .setObjectType(TableSchema.ObjectType.MULTI_ATTRIBUTE)
                .addField(TableSchema.FieldType.KEY, "Papers:Paper ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "URL")
                .build();
        files.put("PaperUrls", schema);

        GradoopElementProcessor processor
                = new GradoopElementProcessor(files.values());

        ExecutorService runner = Executors.newSingleThreadExecutor();

        for (Map.Entry<String, TableSchema> entry : files.entrySet()) {
            runner.submit(new TableFileParser(schema,
                    Paths.get(rootDir, entry.getKey() + ".txt"),
                    processor, PARSE_COUNT));
        }

        runner.submit(() -> ImportMain
                .createGraphFrom(processor, outPath.toAbsolutePath()));
        runner.shutdown();
    }
}
