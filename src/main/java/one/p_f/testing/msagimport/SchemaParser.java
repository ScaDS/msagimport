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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 *
 * @author p-f
 */
public class SchemaParser {

    /**
     * Parse the schema (readme.txt) file to a map of table name and schema.
     *
     * @param sourceFile File to read.
     * @return The map.
     * @throws IOException If some file could not be read as expected.
     */
    public static final Map<String, Schema> parse(Path sourceFile) throws IOException {
        Iterator<String> lines = Files.lines(sourceFile).iterator();
        Map<String, Schema> schemas = new HashMap<>();
        String schemaName = "";
        Schema.Builder builder = new Schema.Builder();
        while (lines.hasNext()) {
            String currentLine = lines.next();
            if (currentLine.startsWith("#")) {
                continue;
            }
            if (currentLine.trim().equals("")) {
                Schema newSchema = builder.build();
                if (newSchema.getColumnCount() != 0) {
                    schemas.put(schemaName, newSchema);
                }
                schemaName = lines.next();
                if (schemaName != null) {
                    schemaName = schemaName.trim().replaceAll("\\s", "");
                }
                builder = new Schema.Builder();
                continue;
            }
            String name = Arrays.stream(currentLine.trim().split("\\t"))
                    .skip(1).collect(Collectors.joining());
            builder.addColumn(name);
        }
        Schema schema = builder.build();
        if (schema.getColumnCount() > 0 && schemaName != null) {
            schemas.put(schemaName, schema);
        }
        return schemas;
    }
}
