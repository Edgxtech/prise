package tech.edgx.prise.indexer.testutil

import com.google.gson.ExclusionStrategy
import com.google.gson.FieldAttributes

class TransactionBodyExcludeStrategy : ExclusionStrategy {
    override fun shouldSkipClass(arg0: Class<*>?): Boolean {
        return false
    }

    override fun shouldSkipField(f: FieldAttributes): Boolean {
        /* No type adapter, need to exclude it */
        return f.name == "certificates"
    }
}