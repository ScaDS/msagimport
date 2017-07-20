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

import java.util.Arrays;

/**
 * Stores a record parsed from the input data and its schema.
 *
 * @author p-f
 */
public class MsagObject {

    /**
     * Schema of this object.
     */
    private final TableSchema schema;

    /**
     * Column data of this record.
     */
    private final String[] data;

    /**
     * Create an empty record.
     *
     * @param schema Schema of the new record.
     */
    public MsagObject(TableSchema schema) {
        this.schema = schema;
        data = new String[schema.getFieldTypes().size()];
    }

    /**
     * Get the schema of this record.
     *
     * @return The schema.
     */
    public TableSchema getSchema() {
        return schema;
    }

    /**
     * Get the data of a certain column of this record.
     *
     * @param field Column number.
     * @return The cell data (or null, if not set).
     * @throws IllegalArgumentException iff there is no such column.
     */
    public String getFieldData(int field) {
        if (field < 0 || field > data.length) {
            throw new IllegalArgumentException("Illegal field number:  "
                    + field);
        }
        return data[field];
    }

    /**
     * Set data of this record. Number of arguments must match the number of
     * columns in this records {@link TableSchema}.
     *
     * @param fieldData Data to set.
     * @throws IllegalArgumentException iff the number of arguments is
     * incorrect.
     */
    public void setFieldData(String... fieldData) {
        if (fieldData.length != data.length) {
            throw new IllegalArgumentException("Wrong number of fields given");
        }
        System.arraycopy(fieldData, 0, data, 0, data.length);
    }

    /**
     * Set data of a certain column of this record.
     * 
     * @param field Number of the column to set.
     * @param newData Data to put in the cell.
     * @throws IllegalArgumentException iff the column number is not in range.
     */
    public void setFieldData(int field, String newData) {
        if (field < 0 || field > data.length) {
            throw new IllegalArgumentException("Illegal field number:  "
                    + field);
        }
        data[field] = newData;
    }

    @Override
    public String toString() {
        return schema.getType().toString() + ' ' + schema.getSchemaName() + ':'
                + Arrays.deepToString(data);
    }
}
