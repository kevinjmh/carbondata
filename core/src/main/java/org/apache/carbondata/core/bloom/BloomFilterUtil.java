/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.bloom;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.roaringbitmap.RoaringBitmap;

public class BloomFilterUtil {

  /**
   *  Calculate bloom parameters by item size and fpp
   *  Formula:
   *     Number of bits (m) = -n * ln(p) / (ln(2)^2)
   *     Number of hashes(k) = m / n * ln(2)
   *
   * @param n Number of items in the filter
   * @param p False positive probability
   *
   */
  public static int[] getBloomParameters(int n, double p) {
    double numOfBit = -n * Math.log(p) / (Math.pow(Math.log(2), 2));
    double numOfHash = numOfBit / n * Math.log(2);
    return new int[]{(int) Math.ceil(numOfBit), (int) Math.ceil(numOfHash)};
  }

  /**
   * Get bitset from super class using reflection, in some cases java cannot access
   * the fields if jars are loaded in separate class loaders.
   *
   */
  public static BitSet getBitSet(BloomFilter bf) throws IOException {
    try {
      Field field = BloomFilter.class.getDeclaredField("bits");
      field.setAccessible(true);
      return (BitSet)field.get(bf);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Set bitset from super class using reflection, in some cases java cannot access
   * the fields if jars are loaded in separte class loaders.
   */
  public static void setBitSet(BitSet bitSet, BloomFilter bf) throws IOException {
    try {
      Field field = BloomFilter.class.getDeclaredField("bits");
      field.setAccessible(true);
      field.set(bf, bitSet);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static RoaringBitmap convertBitSetToRoaringBitmap(BitSet bits) {
    RoaringBitmap bitmap = new RoaringBitmap();
    int length = bits.cardinality();
    int nextSetBit = bits.nextSetBit(0);
    for (int i = 0; i < length; ++i) {
      bitmap.add(nextSetBit);
      nextSetBit = bits.nextSetBit(nextSetBit + 1);
    }
    return bitmap;
  }

  public static RoaringBitmap getRoaringBitmap(BloomFilter bf)
          throws IOException {
    return convertBitSetToRoaringBitmap(getBitSet(bf));
  }

  /**
   * return default null value based on datatype. This method refers to ColumnPage.putNull
   *
   * Note: since we can not mark NULL with corresponding data type in bloom datamap
   * we set/get a `NullValue` for NULL, such that pruning using bloom filter
   * will have false positive case if filter value is the `NullValue`.
   * This should not affect the correctness of result
   */
  public static Object getNullValueForMeasure(DataType dataType, int scale) {
    if (dataType == DataTypes.BOOLEAN) {
      return false;
    } else if (dataType == DataTypes.BYTE) {
      return (byte) 0;
    } else if (dataType == DataTypes.SHORT) {
      return (short) 0;
    } else if (dataType == DataTypes.INT) {
      return 0;
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
      return 0L;
    } else if (dataType == DataTypes.DOUBLE) {
      return 0.0;
    } else if (DataTypes.isDecimal(dataType)) {
      // keep consistence with `DecimalConverter.getDecimal` in loading process
      return BigDecimal.valueOf(0, scale);
    } else {
      throw new IllegalArgumentException("unsupported data type: " + dataType);
    }
  }

  /**
   * get raw bytes from LV structure, L is short encoded
   */
  public static byte[] getRawBytes(byte[] lvData) {
    byte[] indexValue = new byte[lvData.length - CarbonCommonConstants.SHORT_SIZE_IN_BYTE];
    System.arraycopy(lvData, CarbonCommonConstants.SHORT_SIZE_IN_BYTE,
            indexValue, 0, lvData.length - CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    return indexValue;
  }

  /**
   * get raw bytes from LV structure, L is int encoded
   */
  public static byte[] getRawBytesForVarchar(byte[] lvData) {
    byte[] indexValue = new byte[lvData.length - CarbonCommonConstants.INT_SIZE_IN_BYTE];
    System.arraycopy(lvData, CarbonCommonConstants.INT_SIZE_IN_BYTE,
            indexValue, 0, lvData.length - CarbonCommonConstants.INT_SIZE_IN_BYTE);
    return indexValue;
  }
}
