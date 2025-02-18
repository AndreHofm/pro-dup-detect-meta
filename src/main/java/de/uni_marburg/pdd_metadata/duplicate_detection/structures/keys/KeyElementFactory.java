package de.uni_marburg.pdd_metadata.duplicate_detection.structures.keys;

public abstract class KeyElementFactory {
    public KeyElementFactory() {
    }

    public abstract KeyElement create(int var1, String[] var2);

    public abstract KeyElement create(int var1, String var2);
}