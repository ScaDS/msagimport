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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import one.p_f.testing.msagimport.data.TableSchema;
import one.p_f.testing.msagimport.parse.TableFileParser;

/**
 *
 * @author p-f
 */
@Deprecated
public class ImportMain {
    public static void main(String[] args) {
        TableSchema schema = new TableSchema.Builder()
                .setSchemaName("Authors")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Author ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Author name")
                .build();
        Path path = Paths.get(args[0], "Authors.txt");
        TableFileParser parser = new TableFileParser(schema, path,
                o->System.out.println(o.toString()), 10);
        ExecutorService runner = Executors.newSingleThreadExecutor();
        runner.submit(parser);
        runner.shutdown();
    }
}
