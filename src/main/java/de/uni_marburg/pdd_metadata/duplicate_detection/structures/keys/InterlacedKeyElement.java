package de.uni_marburg.pdd_metadata.duplicate_detection.structures.keys;

public class InterlacedKeyElement extends KeyElement {
    private String interlacedAttributesValue;

    public String getValue() {
        return this.interlacedAttributesValue;
    }

    public InterlacedKeyElement(int id, String[] attributeValues, int interlacedKeyMaxLength) {
        this.id = id;
        this.interlacedAttributesValue = this.interlace(attributeValues, interlacedKeyMaxLength).toLowerCase();
    }

    public InterlacedKeyElement(int id, String value) {
        this.id = id;
        this.interlacedAttributesValue = value;
    }

    public int hashCode() {
        return this.id;
    }

    public boolean equals(Object obj) {
        if (!(obj instanceof InterlacedKeyElement)) {
            return false;
        } else {
            InterlacedKeyElement other = (InterlacedKeyElement) obj;
            return this.id == other.getId();
        }
    }

    public String toString() {
        return this.id + ": " + this.interlacedAttributesValue;
    }

    private String interlace(String[] attributeValues, int interlacedKeyMaxLenght) {
        StringBuffer buffer = new StringBuffer();

        for (int charPos = 0; charPos < interlacedKeyMaxLenght / attributeValues.length; ++charPos) {
            for (String attributeValue : attributeValues) {
                if (attributeValue.length() > charPos) {
                    buffer.append(attributeValue.charAt(charPos));
                } else {
                    buffer.append('#');
                }
            }
        }

        return buffer.toString();
    }
}
