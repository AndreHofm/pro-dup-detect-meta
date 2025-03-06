package de.uni_marburg.pdd_metadata.data_profiling.structures;

import lombok.Getter;
import lombok.Setter;

@Getter
public class AttributeWeight {
    private final int index;
    private final String attribute;

    @Setter
    private double weight = 0;

    public AttributeWeight(int index, String attribute) {
        this.index = index;
        this.attribute = attribute;
    }

    @Override
    public String toString() {
        return "[Index=" + this.index + ", " + "Name=" + this.attribute + ", " + "Weight=" + this.weight + "]";
    }
}
