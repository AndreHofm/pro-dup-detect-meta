package de.pdd_metadata.duplicate_detection;

import de.pdd_metadata.duplicate_detection.structures.KeyElement;
import de.pdd_metadata.duplicate_detection.structures.progressive_blocking.BlockResult;
import de.pdd_metadata.duplicate_detection.structures.Record;
import lombok.Getter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

@Getter
public class Magpie {
    private int[] order;
    private int orderPointer;
    private HashMap<Integer, Record> records;
    private SortedSet<KeyElement> keys;
    private Set<Integer> assignedRecords;
    private int maxManageableKeys;
    private int numDiscardKeys;
    private int numKeys;
    private int numDuplicates;
    private Set<BlockResult> blockResults;
    private int blockOffset;

    public Magpie(int numKeys, int maxManageableKeys, int numDiscardKeys) {
        this.order = new int[numKeys];
        this.orderPointer = 0;
        this.records = new HashMap();
        this.keys = new TreeSet();
        this.assignedRecords = new HashSet();
        this.maxManageableKeys = maxManageableKeys;
        this.numDiscardKeys = numDiscardKeys;
        this.numKeys = numKeys;
        this.numDuplicates = 0;
        this.blockResults = new HashSet();
        this.blockOffset = 0;
    }

    public void collect(KeyElement key) {
        if (!this.assignedRecords.contains(key.getId())) {
            this.keys.add(key);
            if (this.keys.size() > this.maxManageableKeys) {
                this.discardLastKeys();
            }

        }
    }

    public void collectProgressive(KeyElement key, Record record) {
        if (!this.assignedRecords.contains(key.getId())) {
            this.keys.add(key);
            this.records.put(key.getId(), record);
            if (this.keys.size() > this.maxManageableKeys) {
                this.discardLastKeysProgressive();
            }

        }
    }

    public void place() {
        if (this.orderPointer + this.keys.size() < this.numKeys) {
            this.discardLastKeys();
        }

        this.placeKeys();
    }

    public void placeProgressive(Blocking blocking, int keyId) {
        if (this.orderPointer + this.keys.size() < this.numKeys) {
            this.discardLastKeysProgressive();
            this.discardLastKeysProgressiveToMultipleOf(blocking.getBlockSize());
        }

        int placedKeys = this.placeKeys();
        if (!this.records.isEmpty()) {
            this.blockResults.addAll(blocking.findDuplicatesIn(this.records, Arrays.copyOfRange(this.order, this.orderPointer - placedKeys, this.orderPointer), keyId, this.blockOffset));
            this.records.clear();
            this.blockOffset += placedKeys / blocking.getBlockSize();
        }

    }

    public boolean isFull() {
        return this.orderPointer == this.numKeys;
    }

    private void discardLastKeys() {
        for (int i = 0; i < this.numDiscardKeys; ++i) {
            KeyElement highestKey = (KeyElement) this.keys.last();
            this.keys.remove(highestKey);
        }

    }

    private void discardLastKeysProgressive() {
        for (int i = 0; i < this.numDiscardKeys; ++i) {
            KeyElement highestKey = (KeyElement) this.keys.last();
            this.keys.remove(highestKey);
            this.records.remove(highestKey.getId());
        }

    }

    private void discardLastKeysProgressiveToMultipleOf(int blockSize) {
        while (this.keys.size() % blockSize != 0) {
            KeyElement highestKey = (KeyElement) this.keys.last();
            this.keys.remove(highestKey);
            this.records.remove(highestKey.getId());
        }

    }

    private int placeKeys() {
        int placedKeys = this.keys.size();

        for (KeyElement key : this.keys) {
            this.assignedRecords.add(key.getId());
            this.order[this.orderPointer] = key.getId();
            ++this.orderPointer;
        }

        this.keys.clear();
        return placedKeys;
    }

    private void tossLast() {
        --this.orderPointer;
        this.assignedRecords.remove(this.order[this.orderPointer]);
    }
}
