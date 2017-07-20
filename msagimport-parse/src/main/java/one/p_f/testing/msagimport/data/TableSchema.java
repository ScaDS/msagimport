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
package one.p_f.testing.msagimport.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 *
 * @author p-f
 */
public class TableSchema {

    public static final class Builder {

        private String schemaName;
        private ObjectType type;
        private final List<FieldType> fieldTypes;
        private final List<String> fieldNames;

        public Builder() {
            schemaName = null;
            fieldTypes = new ArrayList<>();
            fieldNames = new ArrayList<>();
        }

        public Builder setObjectType(ObjectType type) {
            this.type = type;
            return this;
        }

        public Builder setSchemaName(String name) {
            schemaName = name;
            return this;
        }

        public Builder addField(FieldType type, String name) {
            fieldTypes.add(type);
            fieldNames.add(name);
            return this;
        }

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

    public static enum ObjectType {
        NODE,
        EDGE,
        EDGE_3,
        MULTI_ATTRIBUTE
    }

    public static enum FieldType {
        ID,
        ATTRIBUTE,
        ATTRIBUTE_1,
        KEY,
        KEY_1,
        KEY_2,
        IGNORE
    }

    public static char SCOPE_SEPARATOR = ':';

    private TableSchema() {
    }

    private String schemaName;

    private ObjectType type;

    private List<FieldType> fieldTypes;

    private List<String> fieldNames;

    /**
     * @return the schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * @return the type
     */
    public ObjectType getType() {
        return type;
    }

    /**
     * @return the fieldTypes
     */
    public List<FieldType> getFieldTypes() {
        return fieldTypes;
    }

    /**
     * @return the fieldNames
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
