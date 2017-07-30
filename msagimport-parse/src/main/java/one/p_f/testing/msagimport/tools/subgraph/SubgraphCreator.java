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
package one.p_f.testing.msagimport.tools.subgraph;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Main class for creating the subgraph.
 *
 * @author p-f
 */
public class SubgraphCreator {

    /**
     * Logger of this class.
     */
    private static final Logger LOG
            = Logger.getLogger(SubgraphCreator.class.getName());

    /**
     * Options for output files.
     */
    private static final OpenOption[] WRITE_BEHAVIOR = {
        StandardOpenOption.CREATE_NEW, StandardOpenOption.TRUNCATE_EXISTING
    //StandardOpenOption.APPEND, StandardOpenOption.CREATE
    };

    /**
     * Filter lines of a file by a predicate and write it to another file.
     *
     * @param in Path containing file to read.
     * @param out Path to write file to.
     * @param table Table file to filter (without .txt suffix).
     * @param filter Predicate to filter by.
     * @return Number of matching lines.
     */
    private static int filterBy(Path in, Path out, String table,
            Predicate<String> filter) {
        LOG.log(Level.INFO, "Filtering {0}.", table);
        Stream<String> lines;
        try {
            lines = Files.lines(in.resolve(table + ".txt"));
        }
        catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to read source file.", ex);
            lines = Stream.of();
        }
        List<String> resultLines = lines.filter(filter)
                .collect(Collectors.toList());
        LOG.log(Level.INFO, "Matched {0} lines.", resultLines.size());
        try {
            Files.write(out.resolve(table + ".txt"), resultLines,
                    WRITE_BEHAVIOR);
        }
        catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to write file.", ex);
            return 0;
        }
        return resultLines.size();
    }

    /**
     * Filter a table comparing a column of the first table with a column of the
     * second table and writing back the second table.
     *
     * @param in Path to read from.
     * @param out Path to write to.
     * @param sourceTable Table to read the filter from.
     * @param targetTable Table to filter.
     * @param sourceColumn Column of filter table.
     * @param targetColumn Column of table to filter.
     * @throws RuntimeException iff the filter did not match anything.
     */
    private static void filterByColumn(Path in, Path out, String sourceTable,
            String targetTable, int sourceColumn, int targetColumn) {
        LOG.log(Level.INFO, "Filtering {0} [{1}] by {2} [{3}]",
                new Object[]{targetTable, targetColumn, sourceTable,
                    sourceColumn});
        Set<String> filter = readColumn(out.resolve(sourceTable + ".txt"),
                sourceColumn);
        filterBy(in, out, targetTable, e -> filter
                .contains(e.split("\\t")[targetColumn]));
    }

    /**
     * Create a symbolic link at target directing to source. Copies the file iff
     * symbolic links are not supported.
     *
     * @param source File to create the link to.
     * @param target Path to put the link.
     * @return true iff the link was created or the file was copied.
     */
    private static boolean linkOrCopy(Path source, Path target) {
        LOG.log(Level.INFO, "Creating link: {0} -> {1}",
                new Object[]{source.toString(), target.toString()});
        // TODO: Make the link relative.
        try {
            Files.createSymbolicLink(target, source);
            return true;
        }
        catch (IOException ex) {
            LOG.log(Level.WARNING, "Failed to create link.", ex);
        }
        try {
            Files.copy(source, target);
            return true;
        }
        catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to copy file.", ex);
        }
        return false;
    }

    /**
     * Read a certain column of a file.
     *
     * @param of File to read.
     * @param column Column to read.
     * @return The set of columns.
     * @throws RuntimeException if reading or parsing the file failed.
     */
    private static Set<String> readColumn(Path of, int column) {
        LOG.log(Level.INFO, "Reading {0} as a filter.", of.toString());
        try {
            return Files.lines(of).map(l -> l.split("\\t", -1)[column])
                    .collect(Collectors.toCollection(HashSet::new));
        }
        catch (IOException ex) {
            LOG.log(Level.SEVERE, "Failed to read file.", ex);
            throw new RuntimeException(ex);
        }
        catch (ArrayIndexOutOfBoundsException aioobe) {
            LOG.log(Level.SEVERE, "Not enough columns in file.", aioobe);
            throw new RuntimeException(aioobe);
        }
    }

    /**
     * Does not filter a file.
     *
     * @param source Source path.
     * @param target Target path.
     * @param fileName Table to skip.
     */
    private static void skip(Path source, Path target, String fileName) {
        LOG.log(Level.INFO, "Skipping filter for {0}", fileName);
        if (!linkOrCopy(source.resolve(fileName), target.resolve(fileName))) {
            throw new RuntimeException("Failed to skip file: " + fileName);
        }
    }

    /**
     * Main method. Command line arguments are the source path (graph root) the
     * target path and the filter for Affiliations.
     *
     * @param args Usage: INPATH OUTPUT FILTER
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: INPATH OUTPATH FILTER");
            return;
        }
        Path inPath = Paths.get(args[0]);
        Path outPath = Paths.get(args[1]);
        if ((!Files.exists(inPath)) || !Files.isDirectory(inPath)) {
            System.err.println("INPATH must be a directory.");
            return;
        }
        skip(inPath, outPath, "FieldsOfStudy.txt");
        skip(inPath, outPath, "FieldOfStudyHierarchy.txt");
        filterBy(inPath, outPath, "Affiliations", e
                -> e.toLowerCase().contains(args[2].toLowerCase()));
        filterByColumn(inPath, outPath, "Affiliations",
                "PaperAuthorAffiliations", 0, 2);
        filterByColumn(inPath, outPath, "PaperAuthorAffiliations", "Papers", 0,
                0);
        filterByColumn(inPath, outPath, "PaperAuthorAffiliations", "Authors", 1,
                0);
        filterByColumn(inPath, outPath, "Papers", "Journals", 8, 0);
        filterByColumn(inPath, outPath, "Papers", "Conferences", 9, 0);
        filterByColumn(inPath, outPath, "Papers", "PaperKeywords", 0, 0);
        filterByColumn(inPath, outPath, "Papers", "PaperReferences", 0, 0);
        filterByColumn(inPath, outPath, "Conferences", "ConferenceInstances", 0,
                0);
        filterByColumn(inPath, outPath, "Papers", "PaperUrls", 0, 0);
    }
}
