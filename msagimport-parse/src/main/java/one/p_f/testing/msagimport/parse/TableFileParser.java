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
package one.p_f.testing.msagimport.parse;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import one.p_f.testing.msagimport.callback.ElementProcessor;
import one.p_f.testing.msagimport.data.MsagObject;
import one.p_f.testing.msagimport.data.TableSchema;

/**
 * Parses a file using a given {@link TableSchema}.
 *
 * @author p-f
 */
public class TableFileParser implements Runnable {

    /**
     * Maximum number of lines to parse.
     */
    private final long maxParseCount;

    /**
     * The processor to handle the resulting {@link MsagObject}s.
     */
    private final ElementProcessor processor;

    /**
     * The schema of the input file.
     */
    private final TableSchema targetSchema;

    /**
     * Path of the imput file.
     */
    private final Path source;

    /**
     * Initialize a parser.
     *
     * @param schema Schema of the input file.
     * @param source Path of the input file.
     * @param processor Processor to handle the resulting {@link MsagObject}s.
     * @param maxCount Maximum number of lines to parse.
     */
    public TableFileParser(TableSchema schema, Path source,
            ElementProcessor processor, long maxCount) {
        this.processor = processor;
        targetSchema = schema;
        this.source = source;
        maxParseCount = maxCount;
    }

    /**
     * Same as {@link TableFileParser#TableFileParser( TableSchema, Path,
     * ElementProcessor, long) TableFileParser(...)} but using
     * {@link Long#MAX_VALUE} as maximum number of lines to parse.
     *
     * @param schema Schema of the input file.
     * @param source Path of the input file.
     * @param processor Processor to handle the resulting {@link MsagObject}s.
     */
    public TableFileParser(TableSchema schema, Path source,
            ElementProcessor processor) {
        this(schema, source, processor, Long.MAX_VALUE);
    }

    @Override
    public void run() {
        Logger.getLogger(TableFileParser.class.getName())
                .info("Running on " + source.toString());
        Stream<String> lines;
        try {
            lines = Files.lines(source);
        }
        catch (IOException ex) {
            Logger.getLogger(TableFileParser.class.getName())
                    .log(Level.SEVERE, null, ex);
            return;
        }
        lines.limit(maxParseCount).map(line -> line.split("\\t"))
                .map(split -> {
                    MsagObject obj = new MsagObject(targetSchema);
                    obj.setFieldData(split);
                    return obj;
                }).forEach(processor::process);
    }

}
