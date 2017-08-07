/**
 * Copyright 2017 The magimport contributers.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.examples.io.mag.magimport;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.gradoop.examples.io.mag.magimport.data.TableSchema;

/**
 * Helper class storing the format of the input csv files.
 */
public final class InputSchema {

    /**
     * The format, as an immutable map.
     */
    private static final Map<String, TableSchema> FORMAT;

    static {
        Map<String, TableSchema> files = new TreeMap<>();

        TableSchema schema = new TableSchema.Builder()
                .setSchemaName("Authors")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Author ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Name")
                .build();
        files.put("Authors", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("Affiliations")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Affiliation ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Name")
                .build();
        files.put("Affiliations", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("ConferenceSeries")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Conference series ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "ShortName")
                .addField(TableSchema.FieldType.ATTRIBUTE, "FullName")
                .build();
        files.put("Conferences", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("ConferenceInstances")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.KEY,
                        "ConferenceSeries:ConferenceSeriesID")
                .addField(TableSchema.FieldType.ID, "Conference instance ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "ShortName")
                .addField(TableSchema.FieldType.ATTRIBUTE, "FullName")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Location")
                .addField(TableSchema.FieldType.ATTRIBUTE, "OfficialURL")
                .addField(TableSchema.FieldType.ATTRIBUTE, "StartDate")
                .addField(TableSchema.FieldType.ATTRIBUTE, "EndDate")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "AbstractRegistrationDate")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "SubmissionDeadlineDate")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "NotificationDueDate")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "FinalVersionDueDate")
                .build();
        files.put("ConferenceInstances", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("FieldsOfStudy")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Field of study ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Name")
                .build();
        files.put("FieldsOfStudy", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("FieldOfStudyHierarchy")
                .setObjectType(TableSchema.ObjectType.EDGE)
                .addField(TableSchema.FieldType.KEY,
                        "FieldsOfStudy:ChildFieldOfStudyID")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "ChildFieldOfStudyLevel")
                .addField(TableSchema.FieldType.KEY,
                        "FieldsOfStudy:ParentFieldOfStufyID")
                .addField(TableSchema.FieldType.ATTRIBUTE,
                        "ParentFieldOfStudyLevel")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Confidence")
                .build();
        files.put("FieldOfStudyHierarchy", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("Journals")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Journal ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Name")
                .build();
        files.put("Journals", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("Papers")
                .setObjectType(TableSchema.ObjectType.NODE)
                .addField(TableSchema.FieldType.ID, "Paper ID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "OriginalTitle")
                .addField(TableSchema.FieldType.IGNORE, "NormalizedTitle")
                .addField(TableSchema.FieldType.ATTRIBUTE, "PublishYear")
                .addField(TableSchema.FieldType.ATTRIBUTE, "PublishDate")
                .addField(TableSchema.FieldType.ATTRIBUTE, "PaperDOI")
                .addField(TableSchema.FieldType.IGNORE, "OriginalVenueName")
                .addField(TableSchema.FieldType.IGNORE, "NormalizedVenueName")
                .addField(TableSchema.FieldType.KEY, "Journals:JournalID")
                .addField(TableSchema.FieldType.KEY,
                        "ConferenceSeries:ConferenceSeriesID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "PaperRank")
                .build();
        files.put("Papers", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperAuthorAffiliations")
                .setObjectType(TableSchema.ObjectType.EDGE_3)
                .addField(TableSchema.FieldType.KEY_1, "Papers:PaperID")
                .addField(TableSchema.FieldType.KEY, "Authors:AuthorID")
                .addField(TableSchema.FieldType.KEY_2,
                        "Affiliations:AffiliationID")
                .addField(TableSchema.FieldType.IGNORE, "OriginalName")
                .addField(TableSchema.FieldType.IGNORE, "NormalizedName")
                .addField(TableSchema.FieldType.ATTRIBUTE_1,
                        "AuthorSequenceNumber")
                .build();
        files.put("PaperAuthorAffiliations", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperKeywords")
                .setObjectType(TableSchema.ObjectType.EDGE)
                .addField(TableSchema.FieldType.KEY, "Papers:PaperID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "Name")
                .addField(TableSchema.FieldType.KEY,
                        "FieldsOfStudy:FieldOfStudyID")
                .build();
        files.put("PaperKeywords", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperReferences")
                .setObjectType(TableSchema.ObjectType.EDGE)
                .addField(TableSchema.FieldType.KEY, "Papers:CitationID")
                .addField(TableSchema.FieldType.KEY, "Papers:ReferenceID")
                .build();
        files.put("PaperReferences", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperUrls")
                .setObjectType(TableSchema.ObjectType.MULTI_ATTRIBUTE)
                .addField(TableSchema.FieldType.KEY, "Papers:PaperID")
                .addField(TableSchema.FieldType.ATTRIBUTE, "URL")
                .build();
        files.put("PaperUrls", schema);

        FORMAT = Collections.unmodifiableMap(files);
    }

    /**
     * Get the input format as a map assigning a {@link TableSchema} to each
     * file of the input data.
     *
     * @return The format.
     */
    public static Map<String, TableSchema> get() {
        return FORMAT;
    }
}
