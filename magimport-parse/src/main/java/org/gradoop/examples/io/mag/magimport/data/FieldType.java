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
package org.gradoop.examples.io.mag.magimport.data;

/**
 * Type of a column.
 */
public enum FieldType {
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
     * Foreign key to a different schema. A value of this column should have the
     * format SCHEMANAME:ID where SCHEMANAME is
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
