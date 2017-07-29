/*
 * Copyright 2017 TraderJoe95 <johannes.leupold@schie-le.de>.
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
package one.p_f.testing.msagimport.grouping.transformation;

import java.util.Arrays;
import java.util.stream.StreamSupport;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.transformation.Transformation;

/**
 * Implements a graph transformation to convert semicolon separated attribute
 * lists back to single attributes.
 *
 * @author TraderJoe95
 */
public class SplitAttributes {

    /**
     * Transformation internally used.
     */
    private Transformation trans;

    /**
     * Creates a new <code>SplitAttributes</code> instance.
     */
    public SplitAttributes() {
        // Function that splits the attribute list on vertices
        TransformationFunction<Vertex> vertexFunc = (c, t) -> {
            Properties pOld = c.getProperties();
            String joined = pOld.get("attributes").getString();

            Properties p = new Properties();
            Arrays.stream(joined.split(";"))
                    .filter(str -> !str.startsWith("@") && !str.equals(""))
                    .map(str -> str.replace(" ", "_"))
                    .map(str -> str.split("@")).sequential()
                    .forEach(arr -> p.set(arr[0], arr[1]));
            StreamSupport.stream(pOld.getKeys().spliterator(), false)
                    .filter(k -> !k.equals("attributes"))
                    .forEach(k -> p.set(k, pOld.get(k)));
            c.setProperties(p);

            return c;
        };

        // Function that splits the attribute list on edges
        TransformationFunction<Edge> edgeFunc = (c, t) -> {
            Properties pOld = c.getProperties();
            String joined = pOld.get("attributes").getString();

            Properties p = new Properties();
            Arrays.stream(joined.split(";"))
                    .filter(str -> !str.startsWith("@") && !str.equals(""))
                    .map(str -> str.replace(" ", "_"))
                    .map(str -> str.split("@")).sequential()
                    .forEach(arr -> p.set(arr[0], arr[1]));
            StreamSupport.stream(pOld.getKeys().spliterator(), false)
                    .filter(k -> !k.equals("attributes"))
                    .forEach(k -> p.set(k, pOld.get(k)));
            c.setProperties(p);

            return c;
        };

        trans = new Transformation(null, vertexFunc, edgeFunc);
    }

    /**
     * Executes transformation on <code>LogicalGraph</code>.
     *
     * @param graph input graph
     * @return result of transformation
     */
    public LogicalGraph execute(LogicalGraph graph) {
        return trans.execute(graph);
    }
}
