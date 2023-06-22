takes an input list like

## Variant Data

The following table displays a list of genetic variants obtained from the input file. Each variant is associated with specific information such as chromosome, start position, allele ID, reference allele (Ref.), alternative allele (Alt.), and the corresponding OMIM ID (Online Mendelian Inheritance in Man).

This data is extracted from a XML file using a parsing script. The purpose of this script is to identify and retrieve relevant information about genetic variants and their associated OMIM IDs.

### Input Data

The input file used for parsing the variants is `input_file.txt`. It contains the following variants:

| chr | start   | alleleID | Ref. | Alt.     |
| --- | ------- | -------- | ---- | -------- |
| 1   | 1273909 | 359205   | G    | GGCATTGGC |
| 1   | 1167897 | 442612   | G    | A        |
| 1   | 986689  | 495050   | C    | CT       |
| ... (remaining variants) |

### Output Data

The parsed variants are matched with their corresponding OMIM IDs, resulting in the following table:

| chr | start   | alleleID | Ref. | Alt.     | OMIM ID |
| --- | ------- | -------- | ---- | -------- | ------- |
| 1   | 1273909 | 359205   | G    | GGCATTGGC | 300912  |
| 1   | 1167897 | 442612   | G    | A        | 176270  |
| 1   | 986689  | 495050   | C    | CT       | 616580  |
| ... (remaining variants) |

