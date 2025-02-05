package de.pdd_metadata.duplicate_detection.structures;

import lombok.Getter;

@Getter
public class Record {
    public int index = -1;
    public int blockingKey;
    public char typeSet;
    public String[] values;
    public int size;

    public Record(int index, String[] values) {
        this.index = index;
        this.values = values;
        this.size = values.length;
    }

    public Record(String[] values) {
        this.values = values;
        this.size = values.length;
    }
}