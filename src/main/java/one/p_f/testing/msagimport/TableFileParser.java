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
