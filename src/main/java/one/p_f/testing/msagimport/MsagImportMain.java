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
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 *
 * @author p-f
 */
public class MsagImportMain {

    private static final int THREADS_MAX = 1;

    public static final Logger LOG
            = Logger.getLogger(MsagImportMain.class.getName());

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            LOG.warning("No argument given.");
            System.exit(1);
        }
        Path schemaPath = Paths.get(args[0]).toAbsolutePath();
        Path dataLocation = schemaPath.getParent();
        if (dataLocation == null) {
            LOG.severe("Data location not found. Exiting.");
            System.exit(1);
        }
        FileSystem directory = dataLocation.getFileSystem();
        Map<String, Schema> schemas = SchemaParser.parse(schemaPath);
        ExecutorService exec = Executors.newFixedThreadPool(THREADS_MAX);
        for (String file : schemas.keySet()) {
            Path filePath = directory.getPath(dataLocation.toString(), file + ".txt");
            Schema fileSchema = schemas.get(file);
            exec.execute(new TableFileParser(filePath, fileSchema, System.out));
        }
        exec.shutdown();
    }

}
