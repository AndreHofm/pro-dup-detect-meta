package de.uni_marburg.pdd_metadata.utils;

import lombok.Getter;
import lombok.Setter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Getter
public class Configuration {
    private final PairSelectionAlgorithm ALGORITHM = PairSelectionAlgorithm.SNM;
    private final boolean USE_PROFILER = true;
    private final boolean USE_WEIGHTS = true;

    private final boolean USE_MISSING_INFORMATION = true;
    private final boolean USE_FD_INFORMATION = true;
    private final boolean USE_UCC_INFORMATION = true;
    private final boolean USE_PK_INFORMATION = true;
    private final boolean USE_IND_INFORMATION = true;

    private String datasetName;
    private String fileName;
    private String goldStandardDatasetName;
    private String goldStandardFileName;
    private final String fileType = ".csv";
    private boolean twoInOneDataset;
    private int[] similarityAttributes;

    private final int partitionSize = 2000000;
    private int blockSize = 4;
    private int maxBlockRange = 4;
    private int windowSize = 20;
    private int windowInterval = 1;

    private double simThreshold;
    @Setter
    private double nullThreshold;
    @Setter
    private double gpdepThreshold;
    @Setter
    private double indThreshold;
    @Setter
    private int maxDeterminant;

    private char attributeSeparator;
    private boolean hasHeadline;
    private final int maxAttributes = 100;
    private Charset charset;
    private final int numLines = 0;

    private final int levenshteinMaxAttributeLength = 200;

    private final int interlacedKeyMaxLength = 100;

    public enum Dataset {
        CD,
        CENSUS,
        CORA,
        DBLP_SCHOLAR,
        NCVOTERS,
        NCVOTERS_SAMPLE,
    }

    public enum PairSelectionAlgorithm {
        SNM,
        BLOCKING,
    }

    public void setDataset(Dataset dataset) {
        switch (dataset) {
            case CD:
                this.datasetName = "cd";
                this.fileName = this.datasetName + fileType;
                this.goldStandardDatasetName = "cd_gold";
                this.goldStandardFileName = this.goldStandardDatasetName + fileType;
                this.blockSize = 4;
                this.maxBlockRange = 4;
                this.windowSize = 20;
                this.windowInterval = 1;
                this.simThreshold = 0.65; // current best 0.65
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                this.twoInOneDataset = false;
                this.similarityAttributes = new int[]{2, 3, 8, 9};
                this.nullThreshold = 0.05; // best overall 0.05
                this.gpdepThreshold = 0;
                this.indThreshold = 0.7;
                this.maxDeterminant = 2;
                break;

            case CENSUS:
                this.datasetName = "census";
                this.fileName = this.datasetName + fileType;
                this.goldStandardDatasetName = "census_DPL";
                this.goldStandardFileName = this.goldStandardDatasetName + fileType;
                this.blockSize = 4;
                this.maxBlockRange = 4;
                this.windowSize = 20;
                this.windowInterval = 1;
                this.simThreshold = 0.75; // current best 0.75
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                this.similarityAttributes = new int[]{1, 5};
                this.nullThreshold = 0.25; // best overall 0.25
                this.gpdepThreshold = 0;
                this.indThreshold = 0.7;
                break;

            case CORA:
                this.datasetName = "cora";
                this.fileName = this.datasetName + fileType;
                this.goldStandardDatasetName = "cora_DPL";
                this.goldStandardFileName = this.goldStandardDatasetName + fileType;
                this.blockSize = 4;
                this.maxBlockRange = 4;
                this.windowSize = 200;
                this.windowInterval = 1;
                this.simThreshold = 0.65; // current best 0.65 since recall higher
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                this.similarityAttributes = new int[]{3, 15};
                this.nullThreshold = 0.35; // best overall 0.05 // PB 0.1
                this.gpdepThreshold = 0;
                this.indThreshold = 0.7;
                break;

            case DBLP_SCHOLAR:
                this.datasetName = "dblp_scholar";
                this.fileName = this.datasetName + fileType;
                this.goldStandardDatasetName = "dblp_scholar_DPL";
                this.goldStandardFileName = this.goldStandardDatasetName + fileType;
                this.blockSize = 4;
                this.maxBlockRange = 4;
                this.windowSize = 20;
                this.windowInterval = 1;
                this.simThreshold = 0.65; // current best 0.65
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                this.twoInOneDataset = true;
                this.similarityAttributes = new int[]{1, 4};
                this.nullThreshold = 0.55; // best overall 0.05
                this.gpdepThreshold = 0;
                this.indThreshold = 0.7;
                break;

            case NCVOTERS:
                this.datasetName = "ncvoters";
                this.fileName = this.datasetName + fileType;
                this.goldStandardDatasetName = "ncvoters_DPL";
                this.goldStandardFileName = this.goldStandardDatasetName + fileType;
                this.blockSize = 4;
                this.maxBlockRange = 4;
                this.windowSize = 20;
                this.windowInterval = 1;
                this.simThreshold = 0.65; // current best 0.65
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                this.similarityAttributes = new int[]{26, 31, 47};
                this.nullThreshold = 0.15; // best overall 0.15
                this.gpdepThreshold = 0.01;
                this.indThreshold = 0.7;
                break;

            case NCVOTERS_SAMPLE:
                this.datasetName = "ncvoters_sample";
                this.fileName = this.datasetName + fileType;
                this.goldStandardDatasetName = "ncvoters_DPL";
                this.goldStandardFileName = this.goldStandardDatasetName + fileType;
                this.blockSize = 4;
                this.maxBlockRange = 4;
                this.windowSize = 20;
                this.windowInterval = 1;
                this.simThreshold = 0.65; // current best 0.65
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                this.similarityAttributes = new int[]{26, 31, 47};
                this.nullThreshold = 0.25; // best overall 0.25
                this.gpdepThreshold = 0.01;
                this.indThreshold = 0.7;
                break;
        }
    }
}
