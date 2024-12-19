package de.pdd_metadata.duplicate_detection.structures;

public class Record {
    public int Id;
    public int blockingKey;
    public char typeSet;
    public String[] values;
    public int size;

    public Record(String[] values) {
        this.values = values;
        this.size = values.length;
    }
}