package de.uni_marburg.pdd_metadata.duplicate_detection;

import de.uni_marburg.pdd_metadata.duplicate_detection.structures.AttributeKeyElementFactory;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.Duplicate;
import de.uni_marburg.pdd_metadata.duplicate_detection.structures.KeyElementFactory;
import de.uni_marburg.pdd_metadata.io.DataReader;
import de.uni_marburg.pdd_metadata.similarity_measures.Levenshtein;
import lombok.Getter;
import lombok.Setter;

import java.util.HashSet;
import java.util.Set;

@Getter
@Setter
public abstract class DuplicateDetector {
    protected Levenshtein levenshtein = new Levenshtein();
    protected Sorter sorter;
    protected DataReader dataReader;
    protected int partitionSize;
    protected KeyElementFactory keyElementFactory = new AttributeKeyElementFactory();
    protected Set<Duplicate> duplicates = new HashSet<>();
    protected double threshold;
}
