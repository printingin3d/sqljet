package org.tmatesoft.sqljet.core.table;

import javax.annotation.Nonnull;

import org.tmatesoft.sqljet.core.SqlJetException;
import org.tmatesoft.sqljet.core.SqlJetTransactionMode;
import org.tmatesoft.sqljet.core.table.engine.SqlJetEngine;

public class SqlJetTransactionRunner<U extends SqlJetEngine> {
    private final @Nonnull SqlJetTransactionMode mode;
    private final @Nonnull U db;

    public SqlJetTransactionRunner(@Nonnull SqlJetTransactionMode mode, @Nonnull U db) {
        this.mode = mode;
        this.db = db;
    }

    public <T> T as(ISqlJetTransaction<T, U> op) throws SqlJetException {
        return db.runEngineTransaction(engine -> op.run(db), mode);
    }

    public void asVoid(ISqlJetConsumer<U> op) throws SqlJetException {
        as(db -> {
            op.run(db);
            return null;
        });
    }

    public boolean asBool(ISqlJetBooleanTransaction<U> op) throws SqlJetException {
        return db.runEngineTransactionBool(engine -> op.run(db), mode);
    }

    public double asDouble(ISqlJetDoubleTransaction<U> op) throws SqlJetException {
        return as(db -> Double.valueOf(op.run(db))).doubleValue();
    }

    public long asLong(ISqlJetLongTransaction<U> op) throws SqlJetException {
        return as(db -> Long.valueOf(op.run(db))).longValue();
    }

    public int asInt(ISqlJetIntTransaction<U> op) throws SqlJetException {
        return as(db -> Integer.valueOf(op.run(db))).intValue();
    }
}
