package de.uni_marburg.pdd_metadata.duplicate_detection.structures;

public class AttributeKeyElementFactory extends KeyElementFactory {
    public AttributeKeyElementFactory() {
    }

    public KeyElement create(int id, String[] attributeValues) {
        return new AttributeKeyElement(id, attributeValues[0]);
    }

    public KeyElement create(int id, String value) {
        return new AttributeKeyElement(id, value);
    }
}