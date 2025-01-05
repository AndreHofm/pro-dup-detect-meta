package de.pdd_metadata.duplicate_detection.structures;

public abstract class KeyElement implements Comparable<KeyElement> {
    protected int id;

    public KeyElement() {
    }

    public int getId() {
        return this.id;
    }

    public abstract String getValue();

    public int compareTo(KeyElement anotherKeyElement) {
        int order = this.getValue().compareTo(anotherKeyElement.getValue());
        if (order != 0) {
            return order;
        } else if (this.getId() > anotherKeyElement.getId()) {
            return 1;
        } else {
            return this.getId() < anotherKeyElement.getId() ? -1 : 0;
        }
    }

    public String[] toStringArray() {
        String[] result = new String[]{String.valueOf(this.getId()), String.valueOf(this.getValue())};
        return result;
    }
}
