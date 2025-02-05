package de.uni_marburg.pdd_metadata.utils;

import de.uni_marburg.pdd_metadata.duplicate_detection.Sorter;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.AttributeKeyElementFactory;
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

    private final int partitionSize = 2000000;
    private final int blockSize = 4;
    private final int maxBlockRange = 4;
    private final int windowSize = 20;
    private final int windowInterval = 1;
    private final Sorter sorter = new Sorter();
    private double threshold;

    private char attributeSeparator;
    private boolean hasHeadline;
    private final int maxAttributes = 100;
    private Charset charset;
    private final int numLines = 0;

    private final int levenshteinMaxAttributeLength = 200;

    public enum Dataset {
        CD,
        DBLP_SCHOLAR,
        CORA,
    }

    public void setDataset(Dataset dataset) {
        switch (dataset){
            case CD:
                this.datasetName = "cd";
                this.fileName = this.datasetName + fileType;
                this.goldStandardDatasetName = "cd_gold";
                this.goldStandardFileName = this.goldStandardDatasetName +  fileType;
                this.threshold = 0.7;
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                break;

            case DBLP_SCHOLAR:
                this.datasetName = "dblp_scholar";
                this.fileName = this.datasetName + fileType;
                this.goldStandardDatasetName = "dblp_scholar_DPL";
                this.goldStandardFileName = this.goldStandardDatasetName +  fileType;
                this.threshold = 0.7;
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                break;

            case CORA:
                this.datasetName = "cora";
                this.fileName = this.datasetName + fileType;
                this.goldStandardDatasetName = "cora_DPL";
                this.goldStandardFileName = this.goldStandardDatasetName +  fileType;
                this.threshold = 0.7;
                this.attributeSeparator = ';';
                this.hasHeadline = true;
                this.charset = StandardCharsets.ISO_8859_1;
                break;
        }
    }
}
