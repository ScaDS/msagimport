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
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author p-f
 */
public class MsagObject {

    private static final Logger LOG
            = Logger.getLogger(MsagObject.class.getName());

    private final TableSchema schema;

    private final String[] data;

    public MsagObject(TableSchema schema) {
        this.schema = schema;
        data = new String[schema.getFieldTypes().size()];
    }

    public TableSchema getSchema() {
        return schema;
    }

    public String getFieldData(int field) {
        if (field < 0 || field > data.length) {
            LOG.log(Level.SEVERE, "Illegal field number: {0}", field);
            throw new IllegalArgumentException("Illegal field number:  "
                    + field);
        }
        return data[field];
    }

    public void setFieldData(String... fieldData) {
        if (fieldData.length != data.length) {
            LOG.log(Level.SEVERE, "Wrong number of fields given: {0}",
                    fieldData.length != 0
                            ? Arrays.deepToString(fieldData) : "<empty>");
            throw new IllegalArgumentException("Wrong number of fields given");
        }
        System.arraycopy(fieldData, 0, data, 0, data.length);
    }

    public void setFieldData(int field, String newData) {
        if (field < 0 || field > data.length) {
            LOG.log(Level.SEVERE, "Illegal field number: {0}", field);
            throw new IllegalArgumentException("Illegal field number:  "
                    + field);
        }
        data[field] = newData;
    }

    public String toString() {
        return schema.getType().toString() + ' ' + schema.getSchemaName() + ':'
                + Arrays.deepToString(data);
    }
}
