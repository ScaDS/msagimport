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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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

        public void setObjectType(ObjectType type) {
            this.type = type;
        }

        public void setSchemaName(String name) {
            schemaName = name;
        }

        public void addField(FieldType type, String name) {
            fieldTypes.add(type);
            fieldNames.add(name);
        }

        public TableSchema build() {
            if (schemaName == null) {
                throw new IllegalStateException("No schema name set.");
            }
            TableSchema schema = new TableSchema();
            schema.schemaName = schemaName;
            schema.type = type;
            schema.fieldTypes = fieldTypes
                    .toArray(new FieldType[fieldTypes.size()]);
            schema.fieldNames = fieldNames
                    .toArray(new String[fieldNames.size()]);
            return schema;
        }
    }

    public static enum ObjectType {
        NODE,
        EDGE
    }

    public static enum FieldType {
        ID,
        ATTRIBUTE,
        KEY,
        IGNORE
    }

    private TableSchema() {
    }

    private String schemaName;

    private ObjectType type;

    private FieldType[] fieldTypes;

    private String[] fieldNames;

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
        return Collections.unmodifiableList(Arrays.asList(fieldTypes));
    }

    /**
     * @return the fieldNames
     */
    public List<String> getFieldNames() {
        return Collections.unmodifiableList(Arrays.asList(fieldNames));
    }
}
