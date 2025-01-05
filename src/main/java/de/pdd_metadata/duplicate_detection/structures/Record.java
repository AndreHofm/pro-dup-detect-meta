package de.pdd_metadata.duplicate_detection.structures;

import lombok.Getter;

@Getter
public class Record {
    public int id = -1;
    public int blockingKey;
    public char typeSet;
    public String[] values;
    public int size;

    public Record(int id, String[] values) {
        this.id = id;
        this.values = values;
        this.size = values.length;
    }

    public Record(String[] values) {
        this.values = values;
        this.size = values.length;
    }
}