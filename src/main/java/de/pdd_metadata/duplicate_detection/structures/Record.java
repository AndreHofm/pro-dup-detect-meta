package de.pdd_metadata.duplicate_detection.structures;

public class Record {
    public int id;
    public int blockingKey;
    public char typeSet;
    public String[] values;
    public int size;

    public Record(int id, String[] values) {
        this.id = id;
        this.values = values;
        this.size = values.length;
    }
}