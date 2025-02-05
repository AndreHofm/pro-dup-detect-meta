package de.uni_marburg.pdd_metadata.duplicate_detection.structures;

public class AttributeKeyElement extends KeyElement {
    private String attributeValue;

    public String getValue() {
        return this.attributeValue;
    }

    public AttributeKeyElement(int id, String attributeValue) {
        this.id = id;
        this.attributeValue = attributeValue.toLowerCase();
    }

    public int hashCode() {
        return this.id;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof AttributeKeyElement)) {
            return false;
        } else {
            AttributeKeyElement other = (AttributeKeyElement)obj;
            return this.id == other.getId();
        }
    }

    public String toString() {
        return this.id + ": " + this.attributeValue;
    }
}