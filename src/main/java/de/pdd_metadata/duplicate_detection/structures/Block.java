package de.pdd_metadata.duplicate_detection.structures;

import java.util.ArrayList;

public class Blocks {
    public String blockId;
    public int size;
    public ArrayList<Record> recordA = new ArrayList<>();
    public ArrayList<Record> recordB = new ArrayList<>();;
}
