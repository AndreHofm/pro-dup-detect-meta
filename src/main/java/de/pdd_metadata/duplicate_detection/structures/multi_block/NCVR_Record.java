/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package de.pdd_metadata.duplicate_detection.structures.multi_block;

import de.pdd_metadata.duplicate_detection.structures.Record;


/**
 * @author karap
 */
public class NCVR_Record extends Record {
    public String lastName;
    public String firstName;
    public String poBox;
    public String address;
    public String city;

    public NCVR_Record(String[] values) {
        super(values);
    }

    public static boolean eval(NCVR_Record a, NCVR_Record b) {
        EditDistance ed = new EditDistance();
        return (ed.editDistance(a.lastName, b.lastName) < 3) && (ed.editDistance(a.firstName, b.firstName) < 2) && (ed.editDistance(a.poBox, b.poBox) < 1);
    }

}
