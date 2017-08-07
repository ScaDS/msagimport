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
 * Type of a table.
 */
public enum ObjectType {
    /**
     * A node.
     */
    NODE, /**
     * An edge between 2 nodes.
     */ EDGE, /**
     * An edge between 3 nodes.
     */ EDGE_3, /**
     * Attributes of an object. (many-to-one)
     *
     * lol, doesn't work atm.
     */ MULTI_ATTRIBUTE
    
}
