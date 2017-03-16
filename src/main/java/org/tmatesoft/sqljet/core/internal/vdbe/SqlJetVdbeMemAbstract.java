package org.tmatesoft.sqljet.core.internal.vdbe;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.internal.ISqlJetMemoryPointer;
import org.tmatesoft.sqljet.core.internal.ISqlJetVdbeMem;
import org.tmatesoft.sqljet.core.internal.SqlJetUtility;

public abstract class SqlJetVdbeMemAbstract implements ISqlJetVdbeMem {

    @Override
    public final boolean isNumber() {
        return isInt() || isReal();
    }

    /**
     * Compare the values contained by the two memory cells, returning negative,
     * zero or positive if pMem1 is less than, equal to, or greater than pMem2.
     * Sorting order is NULL's first, followed by numbers (integers and reals)
     * sorted numerically, followed by text ordered by the collating sequence
     * pColl and finally blob's ordered by memcmp().Two NULL values are
     * considered equal by this function.
     * 
     * @throws SqlJetException
     */
    @Override
    public int compareTo(ISqlJetVdbeMem that) {
        /*
         * If one value is NULL, it is less than the other. If both values* are
         * NULL, return 0.
         */
        if (this.isNull() || that.isNull()) {
            return (that.isNull() ? 1 : 0) - (this.isNull() ? 1 : 0);
        }

        /*
         * If one value is a number and the other is not, the number is less.*
         * If both are numbers, compare as reals if one is a real, or as
         * integers* if both values are integers.
         */
        if (this.isNumber() || that.isNumber()) {
            if (!this.isNumber()) {
                return 1;
            }
            if (!that.isNumber()) {
                return -1;
            }
            /* Comparing to numbers as doubles */
            double r1 = this.realValue();
            double r2 = that.realValue();
            return Double.compare(r1, r2);
        }

        /*
         * If one value is a string and the other is a blob, the string is less.
         * * If both are strings, compare using the collating functions.
         */
        if (this.isString() || that.isString()) {
            if (!this.isString()) {
                return 1;
            }
            if (!that.isString()) {
                return -1;
            }

            return this.stringValue().compareTo(that.stringValue());
        }

        ISqlJetMemoryPointer blob1 = this.blobValue();
        ISqlJetMemoryPointer blob2 = that.blobValue();

        /* Both values must be blobs or strings. Compare using memcmp(). */
        int rc = SqlJetUtility.memcmp(blob1, blob2, Integer.min(blob1.getLimit(), blob2.getLimit()));
        if (rc == 0) {
            rc = blob1.getLimit() - blob2.getLimit();
        }
        return rc;
    }

}
