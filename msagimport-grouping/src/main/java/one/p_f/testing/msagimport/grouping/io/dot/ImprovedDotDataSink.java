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

import java.io.IOException;
import org.apache.flink.core.fs.FileSystem;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.impl.GraphTransactions;

/**
 * A {@link DataSink} to replace
 * {@link org.gradoop.flink.io.impl.dot.DOTDataSink} that can handle attributes
 * properly.
 *
 * @author p-f
 */
public class ImprovedDotDataSink extends DOTDataSink implements DataSink {

    /**
     * Path to write the data to.
     */
    private final String path;

    /**
     * Initialize the sink.
     *
     * @param targetPath Path to write data to.
     * @param graphInfo Ignored.
     */
    public ImprovedDotDataSink(String targetPath, boolean graphInfo) {
        super(targetPath, graphInfo);
        path = targetPath;
    }

    @Override
    public void write(GraphTransactions graphTransactions, boolean overWrite)
            throws IOException {
        FileSystem.WriteMode writeMode = overWrite
                ? FileSystem.WriteMode.OVERWRITE
                : FileSystem.WriteMode.NO_OVERWRITE;
        graphTransactions.getTransactions().writeAsFormattedText(path,
                writeMode, new ImprovedDotFileFormat());

    }

}
