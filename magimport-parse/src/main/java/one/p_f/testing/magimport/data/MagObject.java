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
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Stores a record parsed from the input data and its schema.
 *
 * @author p-f
 */
public class MagObject implements Serializable {

    /**
     * Serialization version ID.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Logger of this class.
     */
    private static final Logger LOG
            = Logger.getLogger(MagObject.class.getName());

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
    public MagObject(TableSchema schema) {
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
            LOG.log(Level.SEVERE, "Illegal field number: {0}", field);
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
     * @return This object.
     * @throws IllegalArgumentException iff the number of arguments is
     * incorrect.
     */
    public MagObject setFieldData(String... fieldData) {
        if (fieldData.length != data.length) {
            String error = fieldData.length != 0
                    ? Arrays.deepToString(fieldData) : "<empty>";
            LOG.log(Level.SEVERE,
                    "Wrong number of fields given: expected {0}, got {1}",
                    new Object[]{fieldData.length, error});
            throw new IllegalArgumentException(String.format(
                    "Wrong number of fields given: expected %d, got \"%s\"",
                    fieldData.length, error));
        }
        System.arraycopy(fieldData, 0, data, 0, data.length);
        return this;
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
            LOG.log(Level.SEVERE, "Illegal field number: {0}", field);
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
