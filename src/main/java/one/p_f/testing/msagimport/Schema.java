/* 
 * Copyright (c) 2017, Philip Fritzsche <p-f@users.noreply.github.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * * Redistributions of source code must retain the above copyright notice, this
 *   list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
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
