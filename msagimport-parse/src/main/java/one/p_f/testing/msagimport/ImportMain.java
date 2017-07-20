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
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import one.p_f.testing.msagimport.data.TableSchema;
import one.p_f.testing.msagimport.parse.TableFileParser;

/**
 * Parse msag data and dump it.
 *
 * @author p-f
 * @deprecated Used for testing only.
 */
@Deprecated
public class ImportMain {

    /**
     * Main: Parse data.
     * 
     * @param args Single argument used as the graph root directory.
     */
    public static void main(String[] args) {
        Path graphRoot = Paths.get(args.length == 0 ? "." : args[0]);
        if( !graphRoot.toFile().isDirectory()) {
            System.err.println("Graph root not found.");
            System.out.println("Usage: ImportMain PATH");
        }
        String rootDir = graphRoot.toString();
        
        Map<String, TableSchema> files = new TreeMap<>();

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

        ExecutorService runner = Executors.newSingleThreadExecutor();
        files.entrySet().stream().map((entry) ->
                new TableFileParser(entry.getValue(),
                    Paths.get(rootDir, entry.getKey() + ".txt"),
                    o->System.out.println(o.toString())
                    , 10))
                .forEach(runner::submit);
        runner.shutdown();
    }
}
