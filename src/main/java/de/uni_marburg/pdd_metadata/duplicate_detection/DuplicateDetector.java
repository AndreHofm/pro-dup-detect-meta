package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.duplicate_detection.structures.keys.AttributeKeyElementFactory;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.keys.KeyElementFactory;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.similarity_measures.JaroWinkler;
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
    protected KeyElementFactory keyElementFactory = new AttributeKeyElementFactory(); // InterlacedKeyElementFactory(100);
    // protected KeyElementFactory keyElementFactory = new InterlacedKeyElementFactory(100);
    protected Set<Duplicate> duplicates = new HashSet<>();
    protected double threshold;
    protected JaroWinkler jaroWinkler;

    public DuplicateDetector(DataReader dataReader, ResultCollector resultCollector, Configuration config) {
        this.dataReader = dataReader;
        this.partitionSize = config.getPartitionSize();
        this.threshold = config.getSimThreshold();
        this.sorter = new Sorter();
        this.levenshtein = new Levenshtein(resultCollector, config);
        this.jaroWinkler = new JaroWinkler();
    }
}
