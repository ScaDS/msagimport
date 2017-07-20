# MSAG Import
## Sub Projects
### msagimport-parse
A generic parser for Microsoft Academic Graph. You have to supply a callback class to process the parsed structures.

#### Important classes
All of the following classes are in package `one.p_f.testing.msagimport`.
* `data.MsagObject`: An object containing a parsed structure from the graph. It can be an edge or a node. All attributes are stored in a string array.
* `data.TableSchema`: Describes the structure of one MSAG TSV file. Contains a list of all columns and column types. Can be created via `data.TableSchema.Builder`.
* `callback.ElementProcessor`: The callback interface for processing parsed data.

### msagimport-gradoop
An implementation of the `msagimport-parse` callback interface. Parses the input files via `msagimport-parse` and saves the results in the Gradoop JSON file format.

#### Important classes
All of the following classes are in package `one.p_f.testing.msagimport.gradoop`.
* `GradoopElementProcessor`: Implementation of the `msagimport-parse` callback interface.

### msagimport-grouping
Loads a Gradoop logical graph from the Gradoop JSON file format and uses the `groupBy`-Operator on it. The resulting graph is written in the DOT format.

## Usage
### Step 1: Parse and import graph
```
java one.p_f.testing.msagimport.gradoop.ImportMain <input path> <output path>
```
The **`input path`** should be the path containing the extracted MSAG TSV Files.

The **`output path`** must be a directory and will contain the JSON output files. The directory will be created if it doesn't already exist.

### Step 2: Group graph
```
java one.p_f.testing.msagimport.grouping.GroupingMain <input path> <output path>
```
The **`input path`** should be the output path from step 1 (containing the JSON Files).

The **`output path`** must be a directory and will contain the DOT output files. The directory will be created if it doesn't already exist.

:beer::beer::beer:
