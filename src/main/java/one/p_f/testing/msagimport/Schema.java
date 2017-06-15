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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Strore column information of a file.
 *
 * @author p-f
 */
public class Schema {

    /**
     * Builder for {@link Schema}.
     */
    public static final class Builder {

        private String schemaName;
        private final List<String> columns;

        public Builder() {
            schemaName = null;
            columns = new ArrayList<>();
        }

        public void addColumn(String name) {
            columns.add(name);
        }

        public void setName(String name) {
            schemaName = name;
        }

        public Schema build() {
            if (schemaName == null) {
                throw new IllegalStateException();
            }
            return new Schema(schemaName,
                    columns.stream().toArray(String[]::new));
        }
    }

    /**
     * Schema Name.
     */
    private final String schemaName;

    /**
     * Column names.
     */
    private final String[] columns;

    public Schema(String schemaName, String... names) {
        this.schemaName = schemaName;
        this.columns = (String[]) names;
    }

    /**
     * Get the Name of the Schema.
     *
     * @return the schemaName
     */
    public String getSchemaName() {
        return schemaName;
    }

    /**
     * Get the name of a column.
     *
     * @param index Column number.
     * @return Name of the column.
     */
    public String getColumnName(final int index) {
        return columns[index];
    }

    /**
     * Get the total number of columns.
     *
     * @return Number of columns.
     */
    public int getColumnCount() {
        return columns.length;
    }

    @Override
    public String toString() {
        return schemaName + ": " + Arrays.deepToString(columns);
    }
}
