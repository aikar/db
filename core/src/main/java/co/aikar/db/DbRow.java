/*
 * Copyright (c) 2016-2017 Daniel Ennis (Aikar) - MIT License
 *
 *  Permission is hereby granted, free of charge, to any person obtaining
 *  a copy of this software and associated documentation files (the
 *  "Software"), to deal in the Software without restriction, including
 *  without limitation the rights to use, copy, modify, merge, publish,
 *  distribute, sublicense, and/or sell copies of the Software, and to
 *  permit persons to whom the Software is furnished to do so, subject to
 *  the following conditions:
 *
 *  The above copyright notice and this permission notice shall be
 *  included in all copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 *  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 *  LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 *  OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 *  WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package co.aikar.db;

import java.util.HashMap;

/**
 * TypeDef alias for results with a template return type getter
 * so casting/implicit getInt type calls are not needed.
 */
public class DbRow extends HashMap<String, Object> {
    /**
     * Get the result as proper type.
     * <p/>
     * VALID: Long myLong = row.get("someUnsignedIntColumn");
     * INVALID: String myString = row.get("someUnsignedIntColumn");
     *
     * @param <T>
     * @param column
     * @return Object of the matching type of the result.
     */
    public <T> T get(String column) {
        return (T) super.get(column);
    }
    /**
     * Get the result as proper type., returning default if not found.
     * <p/>
     * VALID: Long myLong = row.get("someUnsignedIntColumn");
     * INVALID: String myString = row.get("someUnsignedIntColumn");
     *
     * @param <T>
     * @param column
     * @return Object of the matching type of the result.
     */
    public <T> T get(String column, T def) {
        T res = (T) super.get(column);
        if (res == null) {
            return def;
        }
        return res;
    }

    public Long getLong(String column) {
        return get(column);
    }

    public Long getLong(String column, Number def) {
        return get(column, def).longValue();
    }

    public Integer getInt(String column) {
        return ((Number) get(column)).intValue();
    }

    public Integer getInt(String column, Number def) {
        return get(column, def).intValue();
    }

    public Float getFloat(String column) {
        return ((Number) get(column)).floatValue();
    }

    public Float getFloat(String column, Number def) {
        return get(column, def).floatValue();
    }


    public Double getDbl(String column) {
        return ((Number) get(column)).doubleValue();
    }

    public Double getDbl(String column, Number def) {
        return get(column, def).doubleValue();
    }

    public String getString(String column) {
        return get(column);
    }

    public String getString(String column, String def) {
        return get(column, def);
    }

    /**
     * Removes a result, returning as proper type.
     * <p/>
     * VALID: Long myLong = row.remove("someUnsignedIntColumn");
     * INVALID: String myString = row.remove("someUnsignedIntColumn");
     *
     * @param <T>
     * @param column
     * @return Object of the matching type of the result.
     */
    public <T> T remove(String column) {
        return (T) super.remove(column);
    }

    /**
     * Removes a result, returning as proper type, returning default if not found
     * <p/>
     * VALID: Long myLong = row.get("someUnsignedIntColumn");
     * INVALID: String myString = row.get("someUnsignedIntColumn");
     *
     * @param <T>
     * @param column
     * @return Object of the matching type of the result.
     */
    public <T> T remove(String column, T def) {
        T res = (T) super.remove(column);
        if (res == null) {
            return def;
        }
        return res;
    }

    public DbRow clone() {
        DbRow row = new DbRow();
        row.putAll(this);
        return row;
    }
}
