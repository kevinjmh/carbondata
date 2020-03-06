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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.hash.Hash;
import org.roaringbitmap.RoaringBitmap;

/**
 * Bloom filter with RoaringBitmap
 */
public class RoaringBloomFilter implements Writable {

  /** Hashing algorithm to use */
  private Hash hashFunction = Hash.getInstance(Hash.MURMUR_HASH);

  /** Bound of hash result */
  private int vectorSize;

  /** Number of hashed values */
  private int nbHash;

  private RoaringBitmap bits;

  /** used for query */
  public RoaringBloomFilter() {
  }

  public RoaringBloomFilter(int vectorSize, int nbHash) {
    if (vectorSize <= 0) {
      throw new IllegalArgumentException("vectorSize must be > 0");
    }
    this.vectorSize = vectorSize;
    this.nbHash = nbHash;
    this.bits = new RoaringBitmap();
  }

  private int[] hash(byte[] b) {
    if (b.length == 0) {
      throw new IllegalArgumentException("key length must be > 0");
    }
    int[] result = new int[nbHash];
    for (int i = 0, initval = 0; i < nbHash; i++) {
      initval = hashFunction.hash(b, initval);
      result[i] = Math.abs(initval % vectorSize);
    }
    return result;
  }

  public void add(byte[] key) {
    if (key == null) {
      throw new NullPointerException("key cannot be null");
    }

    int[] h = hash(key);
    for (int i = 0; i < nbHash; i++) {
      bits.add(h[i]);
    }
  }

  public boolean membershipTest(byte[] key) {
    if (key == null) {
      throw new NullPointerException("key cannot be null");
    }

    int[] h = hash(key);
    for (int i = 0; i < nbHash; i++) {
      if (!bits.contains(h[i])) {
        return false;
      }
    }
    return true;
  }

  public void write(DataOutput out) throws IOException {
    out.writeInt(nbHash);
    out.writeInt(vectorSize);
    bits.serialize(out);
  }

  public void readFields(DataInput in) throws IOException {
    nbHash = in.readInt();
    vectorSize = in.readInt();
    bits = new RoaringBitmap();
    bits.deserialize(in);
  }

}
