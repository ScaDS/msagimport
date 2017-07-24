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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.transformation.Transformation;

/**
 *
 * @author TraderJoe95
 */
public class SplitAttributes {

    private Transformation trans;

    public SplitAttributes() {
        TransformationFunction<Vertex> vertexFunc = (c, t) -> {
            String joined = c.getProperties().get("attributes").getString();

            Properties p = new Properties();
            Arrays.stream(joined.split(";")).filter(str -> !str.equals(""))
                    .map(str -> str.replace(" ", "_")).sequential()
                    .forEach(key -> p.set(key, "value"));
            c.setProperties(p);

            return c;
        };

        TransformationFunction<Edge> edgeFunc = (c, t) -> {
            String joined = c.getProperties().get("attributes").getString();

            Properties p = new Properties();
            Arrays.stream(joined.split(";")).filter(str -> !str.equals(""))
                    .map(str -> str.replace(" ", "_")).sequential()
                    .forEach(key -> p.set(key, "value"));
            c.setProperties(p);

            return c;
        };

        trans = new Transformation(null, vertexFunc, edgeFunc);
    }

    public LogicalGraph execute(LogicalGraph graph) {
        return trans.execute(graph);
    }
}
