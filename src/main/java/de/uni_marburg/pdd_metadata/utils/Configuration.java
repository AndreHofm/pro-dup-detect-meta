package de.uni_marburg.pdd_metadata.utils;

import lombok.Getter;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

@Getter
public class Configuration {
    private String datasetName;
    private String fileName;
    private String goldStandardDatasetName;
    private String goldStandardFileName;
    private final String fileType = ".csv";
    private boolean twoInOneDataset;

    private final int partitionSize = 2000000;
    private int blockSize = 4;
    private int maxBlockRange = 4;
    private int windowSize = 20;
    private int windowInterval = 1;
    private double threshold;

    private char attributeSeparator;
    private boolean hasHeadline;
    private final int maxAttributes = 100;
    private Charset charset;
    private final int numLines = 0;

    private final int levenshteinMaxAttributeLength = 200;

    private final int interlacedKeyMaxLength = 100;

    public enum Dataset {
        CD,
        DBLP_SCHOLAR,
        CORA,
        CENSUS,
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
                this.threshold = 0.7;
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                this.twoInOneDataset = false;
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
                this.threshold = 0.7;
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                this.twoInOneDataset = true;
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
                this.threshold = 0.7;
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
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
                this.threshold = 0.7;
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                break;
        }
    }
}
