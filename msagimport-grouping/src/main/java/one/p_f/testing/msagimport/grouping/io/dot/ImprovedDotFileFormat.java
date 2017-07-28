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

import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.io.impl.dot.functions.DOTFileFormat;
import org.gradoop.flink.representation.transactional.GraphTransaction;

/**
 * Improve the {@link DOTFileFormat} to produce valid and nicer DOT output.
 *
 * @author p-f
 * @see one.p_f.testing.msagimport.grouping.io.dot.ImprovedDotDataSink
 */
public class ImprovedDotFileFormat extends DOTFileFormat {

    private static final char NL = '\n';

    private static final char ATTR_PREFIX = 'a';

    private static final char NODE_PREFIX = 'v';

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

    private static void writeEdges(GraphTransaction tr, StringBuilder sb) {
        for (Edge e : tr.getEdges()) {
            String from = String.format("%c%s", NODE_PREFIX,
                    e.getSourceId().toString());
            String to = String.format("%c%s", NODE_PREFIX,
                    e.getTargetId().toString());
            Stream<Property> propStream = e.getProperties() == null
                    ? Stream.of()
                    : StreamSupport
                            .stream(e.getProperties().spliterator(), false);
            String prop = propStream
                    .map(f -> f.getKey() + " = " + f.getValue())
                    .collect(Collectors.joining("\\n", "\"", "\""));
            sb.append(String.format("%s%s [label=%s,shape=\"Msquare\"];%c",
                    from, to, prop, NL));
            sb.append(String.format("%s -> %s;%c", from, from + to, NL));
            sb.append(String.format("%s -> %s;%c", from + to, to, NL));
        }
    }

    private static void writeVertices(GraphTransaction tr, StringBuilder sb) {
        for (Vertex v : tr.getVertices()) {
            String id = v.getId().toString();
            // Write vertex.
            sb.append(String.format("%c%s [label=\"%s\", shape=\"ellipse\"];%c",
                    NODE_PREFIX, id, v.getLabel(), NL));
            if (v.getProperties() == null) {
                continue;
            }
            for (Property p : v.getProperties()) {
                // Format property key.
                String propKey = p.getKey().replaceAll("\\s+", "_");
                String attrName = String.format("%c%s%c%s", NODE_PREFIX,
                        id, ATTR_PREFIX, propKey);
                // Write property.
                sb.append(String
                        .format("%s [label=\"%s = %s\", shape=\"rect\"];%c",
                                attrName, propKey, p.getValue(), NL));
                // Add edge vertex->property.
                sb.append(String.format("%c%s -> %s;%c", NODE_PREFIX, id,
                        attrName, NL));
            }
        }
    }
}
