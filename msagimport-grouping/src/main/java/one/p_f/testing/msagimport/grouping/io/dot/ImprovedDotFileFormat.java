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
package one.p_f.testing.msagimport.grouping.io.dot;

import java.util.Comparator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.io.impl.dot.functions.DOTFileFormat;
import org.gradoop.flink.representation.transactional.GraphTransaction;

/**
 * Improve the {@link DOTFileFormat} to produce valid and nicer DOT output. This
 * will draw properties of vertices and edges as extra dot nodes.
 *
 * @author p-f
 * @see one.p_f.testing.msagimport.grouping.io.dot.ImprovedDotDataSink
 */
public class ImprovedDotFileFormat extends DOTFileFormat {

    /**
     * Line separator.
     */
    private static final char NL = '\n';

    /**
     * Prefix for attribute node names.
     */
    private static final char ATTR_PREFIX = 'a';

    /**
     * Shape of attribute nodes of vertices.
     */
    private static final String ATTR_SHAPE = "rect";

    /**
     * Prefix for vertex node names.
     */
    private static final char NODE_PREFIX = 'v';

    /**
     * Initialize. This format is reusable, the
     * {@link ImprovedDotFileFormat#format(GraphTransaction) format} method can
     * be called multiple times, their states will be independent.
     */
    public ImprovedDotFileFormat() {
        super(false);
    }

    @Override
    public String format(GraphTransaction transaction) {
        StringBuilder dot = new StringBuilder().append("digraph ")
                .append('g')
                .append(transaction.getGraphHead().getId())
                .append(" {\n");
        writeVertices(transaction, dot);
        writeEdges(transaction, dot);
        dot.append(NL).append('}');
        return dot.toString();
    }

    /**
     * Helper method writing edges as DOT to a {@link StringBuilder}.
     *
     * @param tr Source of edges.
     * @param sb {@link StringBuilder Builder} to write to.
     */
    private static void writeEdges(GraphTransaction tr, StringBuilder sb) {
        for (Edge e : tr.getEdges()) {
            String from = String.format("%c%s", NODE_PREFIX,
                    e.getSourceId().toString());
            String to = String.format("%c%s", NODE_PREFIX,
                    e.getTargetId().toString());
            if (e.getProperties() == null || e.getProperties().isEmpty()) {
                sb.append(String.format("%s -> %s [label=\"%s\"];%c", from,
                        to, e.getLabel(), NL));
                continue;
            }
            String prop = StreamSupport
                    .stream(e.getProperties().spliterator(), false)
                    .sorted(Comparator.comparing(Property::getKey))
                    .map(f -> f.getKey() + ": " + f.getValue())
                    .collect(Collectors.joining("\\l", "", "\\l"));
            sb.append(String.format("%s%s [label=\"{%s|%s}\",shape=record];%c",
                    from, to, e.getLabel().replaceAll(Pattern.quote("|"),
                            "\\|"), prop, NL));
            sb.append(String.format("%s -> %s [arrowhead=none];%c", from,
                    from + to, NL));
            sb.append(String.format("%s -> %s;%c", from + to, to, NL));
        }
    }

    /**
     * Helper method writing vertices to as DOT to a {@link StringBuilder}.
     *
     * @param tr Source of vertices.
     * @param sb {@link StringBuilder Builder} to write to.
     */
    private static void writeVertices(GraphTransaction tr, StringBuilder sb) {
        for (Vertex v : tr.getVertices()) {
            String id = v.getId().toString();
            // Write vertex.
            sb.append(String.format("%c%s [label=\"%s\", shape=\"ellipse\"];%c",
                    NODE_PREFIX, id, v.getLabel(), NL));
            if (v.getProperties() == null) {
                continue;
            }
            String prop = StreamSupport
                    .stream(v.getProperties().spliterator(), false)
                    .sorted(Comparator.comparing(Property::getKey))
                    .map(f -> f.getKey() + ": " + f.getValue())
                    .collect(Collectors.joining("\\l", "\"", "\\l\""));
            sb.append(String.format("%c%s_attr [label=%s,shape=\"%s\"];%c",
                    NODE_PREFIX, id, prop, ATTR_SHAPE, NL));
            sb.append(String.format(
                    "%c%s_attr -> %c%s [arrowhead=diamond, style=dashed];%c",
                    NODE_PREFIX, id, NODE_PREFIX, id, NL));
        }
    }
}
