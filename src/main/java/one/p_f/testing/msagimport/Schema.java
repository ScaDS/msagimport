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

        private final List<String> columns;

        public Builder() {
            columns = new ArrayList<>();
        }

        public void addColumn(String name) {
            columns.add(name);
        }

        public Schema build() {
            return new Schema(columns.stream().toArray(String[]::new));
        }
    }

    /**
     * Column names.
     */
    private final String[] columns;

    public Schema(String... names) {
        columns = (String[]) names;
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
        return Arrays.deepToString(columns);
    }
}
