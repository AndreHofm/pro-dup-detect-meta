package de.pdd_metadata.duplicate_detection.structures;

public abstract class KeyElementFactory {
    public KeyElementFactory() {
    }

    public abstract KeyElement create(int var1, String[] var2);

    public abstract KeyElement create(int var1, String var2);
}