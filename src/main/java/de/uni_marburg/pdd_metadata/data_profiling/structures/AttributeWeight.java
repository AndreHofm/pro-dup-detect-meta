package de.uni_marburg.pdd_metadata.data_profiling.structures;

import lombok.Getter;
import lombok.Setter;

import java.util.Objects;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AttributeWeight that = (AttributeWeight) o;
        return index == that.index && Objects.equals(attribute, that.attribute) && Objects.equals(weight, that.weight);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, attribute, weight);
    }
}
