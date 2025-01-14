package de.pdd_metadata.data_profiling.structures;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class AttributeScore {
    private final int index;
    private final String attribute;
    private double score;
}
