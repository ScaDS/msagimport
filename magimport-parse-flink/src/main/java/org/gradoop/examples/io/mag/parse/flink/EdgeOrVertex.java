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
package org.gradoop.examples.io.mag.parse.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Represents either an {@link ImportVertex} or an {@link ImportEdge}.
 *
 * @param <K> vertex or edge type.
 */
public class EdgeOrVertex<K extends Comparable<K>>
        extends Tuple5<K, K, K, String, Properties> {

    /**
     * A function converting {@link EdgeOrVertex} back to {@link ImportEdge}.
     *
     * @param <K> Edge type.
     */
    public static class ToEdge<K extends Comparable<K>>
            implements MapFunction<EdgeOrVertex<K>, ImportEdge<K>> {

        @Override
        public ImportEdge<K> map(EdgeOrVertex<K> value)
                throws MagParserException {
            if (value.f1 == null || value.f2 == null) {
                throw new MagParserException("Object is not an edge.");
            }
            return new ImportEdge<>(value.f0, value.f1, value.f2,
                    value.f3, value.f4);
        }

    }

    /**
     * A function converting {@link EdgeOrVertex} back to {@link ImportVertex}.
     *
     * @param <K> Vertex type.
     */
    public static class ToVertex<K extends Comparable<K>>
            implements MapFunction<EdgeOrVertex<K>, ImportVertex<K>> {

        @Override
        public ImportVertex<K> map(EdgeOrVertex<K> value)
                throws MagParserException {
            if (value.f1 != null | value.f2 != null) {
                throw new MagParserException("Object may not be a vertex.");
            }
            return new ImportVertex<>(value.f0, value.f3, value.f4);
        }

    }

    /**
     * Constructor for serialization.
     */
    public EdgeOrVertex() {
    }

    /**
     * Constructor for vertices.
     *
     * @param id Vertex id.
     * @param label Vertex label.
     * @param properties Properties.
     */
    public EdgeOrVertex(K id, String label, Properties properties) {
        f0 = id;
        f1 = null;
        f2 = null;
        f3 = label;
        f4 = properties;
    }

    /**
     * Constructor for edges.
     *
     * @param id Edge id.
     * @param source Source vertex id.
     * @param target Target vertex id.
     * @param label Edge label.
     * @param properties Properties.
     */
    public EdgeOrVertex(K id, K source, K target, String label,
            Properties properties) {
        f0 = id;
        f1 = source;
        f2 = target;
        f3 = label;
        f4 = properties;
    }
}
