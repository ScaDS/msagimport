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
import org.gradoop.examples.io.mag.magimport.data.FieldType;
import org.gradoop.examples.io.mag.magimport.data.ObjectType;
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
                .setObjectType(ObjectType.NODE)
                .addField(FieldType.ID, "Author ID")
                .addField(FieldType.ATTRIBUTE, "Name")
                .build();
        files.put("Authors", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("Affiliations")
                .setObjectType(ObjectType.NODE)
                .addField(FieldType.ID, "Affiliation ID")
                .addField(FieldType.ATTRIBUTE, "Name")
                .build();
        files.put("Affiliations", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("ConferenceSeries")
                .setObjectType(ObjectType.NODE)
                .addField(FieldType.ID, "Conference series ID")
                .addField(FieldType.ATTRIBUTE, "ShortName")
                .addField(FieldType.ATTRIBUTE, "FullName")
                .build();
        files.put("Conferences", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("ConferenceInstances")
                .setObjectType(ObjectType.NODE)
                .addField(FieldType.KEY,
                        "ConferenceSeries:ConferenceSeriesID")
                .addField(FieldType.ID, "Conference instance ID")
                .addField(FieldType.ATTRIBUTE, "ShortName")
                .addField(FieldType.ATTRIBUTE, "FullName")
                .addField(FieldType.ATTRIBUTE, "Location")
                .addField(FieldType.ATTRIBUTE, "OfficialURL")
                .addField(FieldType.ATTRIBUTE, "StartDate")
                .addField(FieldType.ATTRIBUTE, "EndDate")
                .addField(FieldType.ATTRIBUTE,
                        "AbstractRegistrationDate")
                .addField(FieldType.ATTRIBUTE,
                        "SubmissionDeadlineDate")
                .addField(FieldType.ATTRIBUTE,
                        "NotificationDueDate")
                .addField(FieldType.ATTRIBUTE,
                        "FinalVersionDueDate")
                .build();
        files.put("ConferenceInstances", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("FieldsOfStudy")
                .setObjectType(ObjectType.NODE)
                .addField(FieldType.ID, "Field of study ID")
                .addField(FieldType.ATTRIBUTE, "Name")
                .build();
        files.put("FieldsOfStudy", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("FieldOfStudyHierarchy")
                .setObjectType(ObjectType.EDGE)
                .addField(FieldType.KEY,
                        "FieldsOfStudy:ChildFieldOfStudyID")
                .addField(FieldType.ATTRIBUTE,
                        "ChildFieldOfStudyLevel")
                .addField(FieldType.KEY,
                        "FieldsOfStudy:ParentFieldOfStufyID")
                .addField(FieldType.ATTRIBUTE,
                        "ParentFieldOfStudyLevel")
                .addField(FieldType.ATTRIBUTE, "Confidence")
                .build();
        files.put("FieldOfStudyHierarchy", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("Journals")
                .setObjectType(ObjectType.NODE)
                .addField(FieldType.ID, "Journal ID")
                .addField(FieldType.ATTRIBUTE, "Name")
                .build();
        files.put("Journals", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("Papers")
                .setObjectType(ObjectType.NODE)
                .addField(FieldType.ID, "Paper ID")
                .addField(FieldType.ATTRIBUTE, "OriginalTitle")
                .addField(FieldType.IGNORE, "NormalizedTitle")
                .addField(FieldType.ATTRIBUTE, "PublishYear")
                .addField(FieldType.ATTRIBUTE, "PublishDate")
                .addField(FieldType.ATTRIBUTE, "PaperDOI")
                .addField(FieldType.IGNORE, "OriginalVenueName")
                .addField(FieldType.IGNORE, "NormalizedVenueName")
                .addField(FieldType.KEY, "Journals:JournalID")
                .addField(FieldType.KEY,
                        "ConferenceSeries:ConferenceSeriesID")
                .addField(FieldType.ATTRIBUTE, "PaperRank")
                .build();
        files.put("Papers", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperAuthorAffiliations")
                .setObjectType(ObjectType.EDGE_3)
                .addField(FieldType.KEY_1, "Papers:PaperID")
                .addField(FieldType.KEY, "Authors:AuthorID")
                .addField(FieldType.KEY_2,
                        "Affiliations:AffiliationID")
                .addField(FieldType.IGNORE, "OriginalName")
                .addField(FieldType.IGNORE, "NormalizedName")
                .addField(FieldType.ATTRIBUTE_1,
                        "AuthorSequenceNumber")
                .build();
        files.put("PaperAuthorAffiliations", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperKeywords")
                .setObjectType(ObjectType.EDGE)
                .addField(FieldType.KEY, "Papers:PaperID")
                .addField(FieldType.ATTRIBUTE, "Name")
                .addField(FieldType.KEY,
                        "FieldsOfStudy:FieldOfStudyID")
                .build();
        files.put("PaperKeywords", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperReferences")
                .setObjectType(ObjectType.EDGE)
                .addField(FieldType.KEY, "Papers:CitationID")
                .addField(FieldType.KEY, "Papers:ReferenceID")
                .build();
        files.put("PaperReferences", schema);

        schema = new TableSchema.Builder()
                .setSchemaName("PaperUrls")
                .setObjectType(ObjectType.MULTI_ATTRIBUTE)
                .addField(FieldType.KEY, "Papers:PaperID")
                .addField(FieldType.ATTRIBUTE, "URL")
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
