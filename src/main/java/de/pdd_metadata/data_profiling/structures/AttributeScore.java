package de.pdd_metadata.data_profiling.structures;

import lombok.Getter;
import lombok.Setter;

@Getter
public class AttributeScore {
    private final int index;
    private final String attribute;

    @Setter
    private double score = -1;

    public AttributeScore(int index, String attribute) {
        this.index = index;
        this.attribute = attribute;
    }

    @Override
    public String toString() {
        return "[Index=" + this.index + ", " + "Name=" + this.attribute + ", " + "Score=" + this.score + "]";
    }
}
