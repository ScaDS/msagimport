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
package one.p_f.testing.msagimport;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 *
 * @author p-f
 */
public class TableFileParser extends Thread {

    private static final Logger LOG
            = Logger.getLogger(TableFileParser.class.getName());

    private final BufferedWriter writer;

    private final Path sourcePath;

    private final Schema tableSchema;

    public TableFileParser(Path source, Schema schema, OutputStream target) {
        writer = new BufferedWriter(new OutputStreamWriter(target));
        sourcePath = source;
        tableSchema = schema;
    }

    @Override
    public void run() {
        Stream<String> lines;
        try {
            lines = Files.lines(sourcePath);
        }
        catch (IOException ex) {
            LOG.log(Level.SEVERE,
                    "Parsing failed for " + sourcePath.toString(), ex);
            return;
        }
        lines.map(line -> line.split("\\t")).map(cols -> {
            StringBuilder objBuilder = new StringBuilder();
            objBuilder.append('{');
            for (int i = 0; i < cols.length; i++) {
                // TODO: Fix format.
                objBuilder.append('"').append(tableSchema.getColumnName(i))
                        .append('"').append(":").append('"')
                        .append(cols[i]).append('"');
                if (i != (cols.length - 1)) {
                    objBuilder.append(',');
                }
            }
            objBuilder.append('}');
            return objBuilder.toString();
        }).forEach(obj -> {
            try {
                writer.append(obj);
            }
            catch (IOException ex) {
                LOG.log(Level.SEVERE, "Failed to write to output stream", ex);
            }
        });
    }
}
