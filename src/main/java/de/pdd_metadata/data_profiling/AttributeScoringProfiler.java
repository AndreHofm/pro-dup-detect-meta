package de.pdd_metadata.data_profiling;

import de.pdd_metadata.data_profiling.structures.AttributeScore;
import lombok.Getter;

import java.util.HashSet;

@Getter
public class AttributeScoringProfiler {
    private HashSet<AttributeScore> attributeScores = new HashSet<>();
    private FDProfiler fdProfiler = new FDProfiler();
    // private INDProfiler indProfiler = new INDProfiler();
    private UCCProfiler uccProfiler = new UCCProfiler();

    public void execute() {

    }
}
