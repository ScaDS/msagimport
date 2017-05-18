/*
 * Copyright (c) 2017, Philip Fritzsche <p-f@users.noreply.github.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
