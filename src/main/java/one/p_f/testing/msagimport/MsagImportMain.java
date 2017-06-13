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
