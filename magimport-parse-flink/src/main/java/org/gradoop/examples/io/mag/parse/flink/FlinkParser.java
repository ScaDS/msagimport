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

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import one.p_f.testing.magimport.data.MagObject;
import one.p_f.testing.magimport.data.TableSchema;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;

/**
 * Parse a MAG TSV file using flink.
 */
public class FlinkParser implements Callable<DataSet<MagObject>> {

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
     * Create the parser.
     *
     * @param inPath Path to read the files from.
     * @param env Environment to execute the parser on.
     * @param graphSchema Schema of the input tsv data.
     */
    public FlinkParser(String inPath, ExecutionEnvironment env,
            Map<String, TableSchema> graphSchema) {
        environment = env;
        rootPath = new Path(inPath);
        this.graphSchema = graphSchema;
    }

    @Override
    public DataSet<MagObject> call() {
        Optional<DataSet<MagObject>> parsed = graphSchema.entrySet().stream()
                .map(e -> createFromInput(e.getKey(), e.getValue()))
                .reduce(DataSet::union);
        return parsed.isPresent() ? parsed.get() : environment.fromElements();
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
}
