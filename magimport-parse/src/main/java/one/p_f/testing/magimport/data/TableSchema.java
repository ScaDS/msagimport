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
package one.p_f.testing.magimport.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Store the schema of a table (its column names and types.)
 *
 * @author p-f
 */
public class TableSchema implements Serializable {

    /**
     * Builder for {@link TableSchema}.
     */
    public static final class Builder {

        /**
         * Name of this schema/table.
         */
        private String schemaName;

        /**
         * Type of this table.
         *
         * @see ObjectType
         */
        private ObjectType type;

        /**
         * Types of the columns.
         *
         * @see FieldType
         */
        private final List<FieldType> fieldTypes;

        /**
         * Name of the columns.
         */
        private final List<String> fieldNames;

        /**
         * Initilize this builder with no columns.
         */
        public Builder() {
            schemaName = null;
            fieldTypes = new ArrayList<>();
            fieldNames = new ArrayList<>();
        }

        /**
         * Set the type of the {@link TableSchema}.
         *
         * @param type The type.
         * @return This {@link Builder}.
         */
        public Builder setObjectType(ObjectType type) {
            this.type = type;
            return this;
        }

        /**
         * Set the name of the {@link TableSchema}.
         *
         * @param name The name.
         * @return This {@link Builder}.
         */
        public Builder setSchemaName(String name) {
            schemaName = name;
            return this;
        }

        /**
         * Add a new column to this schema.
         *
         * @param type {@link FieldType} of the new column.
         * @param name Name of the new column.
         * @return This {@link Builder}.
         */
        public Builder addField(FieldType type, String name) {
            fieldTypes.add(type);
            fieldNames.add(name);
            return this;
        }

        /**
         * Build the {@link TableSchema}.
         *
         * @return The {@link TableSchema}.
         * @throws IllegalStateException iff no schema name was set.
         */
        public TableSchema build() {
            if (schemaName == null) {
                throw new IllegalStateException("No schema name set.");
            }
            TableSchema schema = new TableSchema();
            schema.schemaName = schemaName;
            schema.type = type;
            schema.fieldTypes = Collections.unmodifiableList(fieldTypes);
            schema.fieldNames = Collections.unmodifiableList(fieldNames);
            return schema;
        }
    }
    
    public static final class ForeignKey { 
        public final String scope;
        public final String key;
        public ForeignKey(String scope, String key) { 
          this.scope = scope; 
          this.key = key; 
        }
    }

    /**
     * Type of a table.
     */
    public static enum ObjectType {
        /**
         * A node.
         */
        NODE,
        /**
         * An edge between 2 nodes.
         */
        EDGE,
        /**
         * An edge between 3 nodes.
         */
        EDGE_3,
        /**
         * Attributes of an object. (many-to-one)
         *
         * lol, doesn't work atm.
         */
        MULTI_ATTRIBUTE
    }

    /**
     * Type of a column.
     */
    public static enum FieldType {
        /**
         * Unique identifier of an object.
         */
        ID,
        /**
         * An attribute of an object.
         */
        ATTRIBUTE,
        /**
         * Used in {@link ObjectType#EDGE_3} as an attribute of the first edge.
         * (Ignored on second edge.)
         */
        ATTRIBUTE_1,
        /**
         * Foreign key to a different schema. A value of this column should have
         * the format SCHEMANAME:ID where SCHEMANAME is
         * {@link TableSchema#getSchemaName() schema name} and ID is a column of
         * type {@link FieldType#ID}.
         */
        KEY,
        /**
         * Foreign key to the first table. (Used in {@link ObjectType#EDGE_3}).)
         */
        KEY_1,
        /**
         * Foreign key to the second table. (Used in {@link ObjectType#EDGE_3}.)
         */
        KEY_2,
        /**
         * Ignore this field. *sad face*
         */
        IGNORE
    }

    /**
     * Used as a separator in
     * {@link FieldType#KEY KEY}, {@link FieldType#KEY_1 KEY_1} and
     * {@link FieldType#KEY_2 KEY_2} to separate
     * {@link TableSchema#getSchemaName() schema name} and {@link FieldType#ID}.
     */
    public static char SCOPE_SEPARATOR = ':';

    /**
     * Unused. Only for use in {@link Builder}.
     */
    private TableSchema() {
    }

    /**
     * Name of this schema.
     */
    private String schemaName;

    /**
     * Type of this schema.
     *
     * @see ObjectType
     */
    private ObjectType type;

    /**
     * Type of each column.
     *
     * @see FieldType
     */
    private List<FieldType> fieldTypes;

    /**
     * Name of each column.
     */
    private List<String> fieldNames;

    /**
     * Get the name of this schema.
     *
     * @return The schema name.
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Get the type of this schema.
     *
     * @return The {@link ObjectType}.
     * @see ObjectType
     */
    public ObjectType getType() {
        return type;
    }

    /**
     * Get the list of column types.
     *
     * @return The list of {@link FieldType}s.
     * @see FieldType
     */
    public List<FieldType> getFieldTypes() {
        return fieldTypes;
    }

    /**
     * Get the list of column names.
     *
     * @return The list of column names.
     */
    public List<String> getFieldNames() {
        return fieldNames;
    }

    @Override
    public String toString() {
        return type.toString() + ' ' + schemaName + ':'
                + IntStream.range(0, fieldNames.size())
                        .mapToObj(i -> fieldTypes.get(i).toString() + ' '
                        + fieldNames.get(i))
                        .collect(Collectors.joining(", "));
    }
}
