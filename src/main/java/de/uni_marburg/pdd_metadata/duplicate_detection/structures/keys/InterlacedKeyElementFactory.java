package de.uni_marburg.pdd_metadata.duplicate_detection.structures.keys;

public class InterlacedKeyElementFactory extends KeyElementFactory {
    private int interlacedKeyMaxLength;

    public InterlacedKeyElementFactory(int interlacedKeyMaxLength) {
        this.interlacedKeyMaxLength = interlacedKeyMaxLength;
    }

    public KeyElement create(int id, String[] attributeValues) {
        return new InterlacedKeyElement(id, attributeValues, this.interlacedKeyMaxLength);
    }

    public KeyElement create(int id, String value) {
        return new InterlacedKeyElement(id, value);
    }
}