package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.duplicate_detection.structures.AttributeKeyElementFactory;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.KeyElementFactory;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.similarity_measures.Levenshtein;
import de.uni_marburg.pdd_metadata.utils.Configuration;
import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
public abstract class DuplicateDetector {
    protected Levenshtein levenshtein;
    protected Sorter sorter;
    protected DataReader dataReader;
    protected int partitionSize;
    protected KeyElementFactory keyElementFactory = new AttributeKeyElementFactory();
    protected Set<Duplicate> duplicates = new HashSet<>();
    protected double threshold;

    public DuplicateDetector(DataReader dataReader, Configuration config) {
        this.dataReader = dataReader;
        this.partitionSize = config.getPartitionSize();
        this.threshold = config.getThreshold();
        this.sorter = new Sorter();
        this.levenshtein = new Levenshtein(config.getLevenshteinMaxAttributeLength());
    }
}
