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

import java.util.logging.Level;
import java.util.logging.Logger;
import one.p_f.testing.magimport.InputSchema;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.Path;

/**
 * Main class for importing mag data via flink.
 */
public class ImportMain {

    /**
     * Logger of this class.
     */
    private static final Logger LOG
            = Logger.getLogger(ImportMain.class.getName());

    /**
     * @param args Usage: -i INPATH -o OUTPATH
     */
    public static void main(String[] args) {
        Options cliOptions = new Options();
        cliOptions.addOption("h", "help", false, "Show this help.");
        cliOptions.addRequiredOption("i", "input", true, "Input path.");
        cliOptions.addRequiredOption("o", "output", true, "Output path.");
        CommandLine cliConfig;
        try {
            cliConfig = new DefaultParser().parse(cliOptions, args);
        } catch (ParseException ex) {
            LOG.log(Level.SEVERE, "Failed to parse command line options: {0}",
                    ex.getMessage());
            showHelp(cliOptions);
            return;
        }
        if (cliConfig.hasOption('h')) {
            showHelp(cliOptions);
            return;
        }
        String inPath = cliConfig.getOptionValue('i');
        Path outPath = new Path(cliConfig.getOptionValue('o'));
        ExecutionEnvironment localEnv = ExecutionEnvironment
                .createLocalEnvironment();
        FlinkParser parser
                = new FlinkParser(inPath, localEnv, InputSchema.get());
        parser.call();

    }

    private static void showHelp(Options o) {
        new HelpFormatter().printHelp("java ImportMain", o, true);
    }

}
