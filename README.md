# MAG Import
## Sub Projects
### magimport-parse
A generic parser for Microsoft Academic Graph. You have to supply a callback class to process the parsed structures.  
Also contains a tool to create a subgraph of MAG by filtering certain tables.

#### Important classes
All of the following classes are in package `one.p_f.testing.magimport`.
* `data.MagObject`: An object containing a parsed structure from the graph. It can be an edge or a node. All attributes are stored in a string array.
* `data.TableSchema`: Describes the structure of one MAG TSV file. Contains a list of all columns and column types. Can be created via `data.TableSchema.Builder`.
* `callback.ElementProcessor`: The callback interface for processing parsed data.

### magimport-gradoop
An implementation of the `magimport-parse` callback interface. Parses the input files via `magimport-parse` and saves the results in the Gradoop JSON file format.

#### Important classes
All of the following classes are in package `one.p_f.testing.magimport.gradoop`.
* `GradoopElementProcessor`: Implementation of the `magimport-parse` callback interface.

### magimport-grouping
Loads a Gradoop logical graph from the Gradoop JSON file format and uses the `groupBy`-Operator on it. The attributes of edges and nodes each get count aggregated. The resulting graph is written in the DOT format.

## Usage
### Step 0: Create a subgraph (OPTIONAL)
```
java one.p_f.testing.magimport.tools.subgraph.SubgraphCreator <input path> <output path> <filter>
```
The **`input path`** should be the path containing the MAG TSV files.

The **`output path`** must be a directory to store the filtered MAG TSV files in.

The **`filter`** is a string searched in the `Affiliations` table (case insensitive).

### Step 1: Parse and import graph
```
java one.p_f.testing.magimport.gradoop.ImportMain <input path> <output path> [<parse limit>]
```
The **`input path`** should be the path containing the extracted MAG TSV files.

The **`output path`** must be a directory and will contain the JSON output files. The directory will be created if it doesn't already exist.

The **`parse limit`** limits how many lines per TSV File get parsed. This value is limited by available memory. The default value is 20000.

### Step 2: Group graph
```
java one.p_f.testing.magimport.grouping.GroupingMain <input path> <output path>
```
The **`input path`** should be the output path from step 1 (containing the JSON Files).

The **`output path`** must be a directory and will contain the DOT output files. The directory will be created if it doesn't already exist.
