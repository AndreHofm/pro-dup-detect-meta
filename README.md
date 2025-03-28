# Progressive Duplicate Detection with Metadata

This project implements the **Progressive Duplicate Detection Algorithms** by Papenbrock et al., as described in this [conference paper](https://hpi.de/oldsite/fileadmin/user_upload/fachgebiete/naumann/publications/PDFs/2015_papenbrock_progressive.pdf).

## Enhancements for Bachelor Thesis

In the context of my Bachelor's Thesis, "Progressive Duplicate Detection with Metadata," additional methods were developed to integrate metadata information(missing values, primary keys, functional dependencies, unique column combinations, and inclusion dependencies) into the progressive algorithms. These enhancements aim to select suitable sorting keys by using metadata.

## Usage

To use the enhanced methods described in the thesis, you can change the parameters of the configuration file:

```Java
    // Can be PairSelectionAlgorithm.PSNM or PairSelectionAlgorithm.PB for using 
    // Progressive Sorted Neighborhood Method or Progressive Blocking
    PairSelectionAlgorithm ALGORITHM = PairSelectionAlgorithm.PSNM;
    // Activates or deactivates the use of attribute weighting and filtering profiler
    boolean USE_PROFILER = true;

    // Activates or deactivates the filtering by using missing values info
    boolean FILTER_WITH_MISSING_INFO = true;
    // Activates or deactivates the filtering by using functional dependency info
    boolean FILTER_WITH_FD_INFO = true;
    // Activates or deactivates the filtering by using primary key info
    boolean FILTER_WITH_PK=  true;
    // Activates or deactivates the filtering by using inclusion dependency info
    boolean FILTER_WITH_IND_INFO = true;

    // Activates or deactivates the weighting of attributes
    boolean USE_WEIGHTS = true;
    // Activates or deactivates the use of functional dependency info for weights
    boolean USE_FD_INFO = true;
    // Activates or deactivates the use of unique column combinations info for weights
    boolean USE_UCC_INFO = true;
```
More parameters can be changed for the different datasets. For more information about the progressive 
algorithms parameters look into this [conference paper](https://hpi.de/oldsite/fileadmin/user_upload/fachgebiete/naumann/publications/PDFs/2015_papenbrock_progressive.pdf).

## [Datasets](https://hpi.de/naumann/projects/repeatability/duplicate-detection/mdedup.html) used in Thesis Examples 

- CD
- Census
- Cora
- DBLP-Scholar
- NCVoters
- A sample of NCVoters
