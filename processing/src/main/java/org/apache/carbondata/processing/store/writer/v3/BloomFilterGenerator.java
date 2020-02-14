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

package org.apache.carbondata.processing.store.writer.v3;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.bloom.BloomFilterUtil;
import org.apache.carbondata.core.bloom.RoaringBloomFilter;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.processing.store.CarbonFactDataHandlerModel;
import org.apache.carbondata.processing.store.TablePage;

import org.apache.log4j.Logger;

/**
 * keep track of distinct values of bloom columns and generate bloom filter
 * for block and blocklet level when loading.
 */
public class BloomFilterGenerator {
  private static final Logger LOGGER =
          LogServiceFactory.getLogService(BloomFilterGenerator.class.getName());
  private double blockletFpp;
  private double blockFpp;

  private List<CarbonColumn> indexColumns;
  // temp stat of distinct values in blocklet level
  private List<Set<Object>> blockletDistinctValue;
  // temp stat of distinct values in block level
  private List<Set<Object>> blockDistinctValue;
  private int lastDimColOrdinal;

  public BloomFilterGenerator(CarbonFactDataHandlerModel model) {
    // find columns config to build page bloom
    CarbonTable table = model.getTableSpec().getCarbonTable();
    String pageBloomIncludeColumns = table.getTableInfo().getFactTable().getTableProperties()
            .get(CarbonCommonConstants.BLOOM_INCLUDE);
    if (null != pageBloomIncludeColumns) {
      LOGGER.info("Build bloom for columns: " + pageBloomIncludeColumns);
      String[] pageBloomCols = pageBloomIncludeColumns.trim().split("\\s*,\\s*");
      lastDimColOrdinal = model.getSegmentProperties().getLastDimensionColOrdinal();
      indexColumns = new ArrayList<>(pageBloomCols.length);
      blockletDistinctValue = new ArrayList<>(pageBloomCols.length);
      blockDistinctValue = new ArrayList<>(pageBloomCols.length);
      for (String bloomColumn : pageBloomCols) {
        CarbonColumn col = table.getColumnByName(bloomColumn.toLowerCase());
        indexColumns.add(col);
        blockletDistinctValue.add(new HashSet<>());
        blockDistinctValue.add(new HashSet<>());
      }
      blockletFpp = CarbonCommonConstants.DEFAULT_BLOCKLET_BLOOM_FPP;
      blockFpp = CarbonCommonConstants.DEFAULT_BLOCK_BLOOM_FPP;
    }
  }

  /**
   * keep track of distinct value of bloom columns
   */
  public void addPage(TablePage rawTablePage) {
    if (null == indexColumns) {
      return;
    }
    for (int i = 0; i < indexColumns.size(); i++) {
      Set<Object> distinctVal = blockletDistinctValue.get(i);
      CarbonColumn col = indexColumns.get(i);
      ColumnPage page = rawTablePage.getColumnPage(col.getColName());
      for (int rowId = 0; rowId < page.getPageSize(); rowId++) {
        distinctVal.add(page.getData(rowId));
      }
    }
  }

  /**
   * convert list of bloom filter to mapping from column ordinal to byte buffer because:
   * 1. cannot write list with NULL binary in thrift
   * 2. ordinal is used in query to get bloom filter
   */
  private Map<Integer, ByteBuffer> getBloomByteBuffer(List<RoaringBloomFilter> bloom) {
    Map<Integer, ByteBuffer> result = new HashMap<>();
    for (int i = 0; i < indexColumns.size(); i++) {
      int colOrdinal = indexColumns.get(i).getOrdinal();
      if (indexColumns.get(i).isMeasure()) {
        // measure is ordered after dimension
        colOrdinal += lastDimColOrdinal;
      }
      try {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(bos);
        bloom.get(i).write(dos);
        bos.close();
        ByteBuffer bloomBytes = ByteBuffer.wrap(bos.toByteArray());
        result.put(colOrdinal, bloomBytes);
      } catch (IOException e) {
        // any exception can be taken as no bloom filter is generated
      }
    }
    return result;
  }

  /**
   * update distinct values in block level using stat from blocklet level
   */
  private void updateBlockDistinct() {
    for (int i = 0; i < indexColumns.size(); i++) {
      Set<Object> distinctVal = blockDistinctValue.get(i);
      distinctVal.addAll(blockletDistinctValue.get(i));
    }
  }

  /**
   * get bloom filter for columns in blocklet level
   */
  Map<Integer, ByteBuffer> endBlocklet() {
    if (null == indexColumns) {
      return null;
    }
    List<RoaringBloomFilter> blockletBloom = buildBloom(blockletDistinctValue, blockletFpp);
    updateBlockDistinct();
    // reset blocklet level stat
    for (Set<Object> colDistinctSet : blockletDistinctValue) {
      colDistinctSet.clear();
    }
    return getBloomByteBuffer(blockletBloom);
  }

  /**
   * get bloom filter for columns in block level
   */
  Map<Integer, ByteBuffer> endBlock() {
    if (null == indexColumns) {
      return null;
    }
    List<RoaringBloomFilter> blockBloom = buildBloom(blockDistinctValue, blockFpp);
    // reset block level stat
    for (Set<Object> colDistinctSet : blockDistinctValue) {
      colDistinctSet.clear();
    }
    return getBloomByteBuffer(blockBloom);
  }

  /**
   * create bloom filter for bloom columns with specific false positive probabilities
   */
  private List<RoaringBloomFilter> buildBloom(List<Set<Object>> distinctValues, double fpp) {
    List<RoaringBloomFilter> blockletBloom = new ArrayList<>(indexColumns.size());
    for (int i = 0; i < indexColumns.size(); i++) {
      Set<Object> distinctVal = distinctValues.get(i);
      int[] params = BloomFilterUtil.getBloomParameters(distinctVal.size(), fpp);
      RoaringBloomFilter bloomFilter = new RoaringBloomFilter(params[0], params[1]);
      for (Object data : distinctVal) {
        bloomFilter.add(getBytes(data, indexColumns.get(i)));
      }
      blockletBloom.add(bloomFilter);
    }
    return blockletBloom;
  }

  /**
   * convert value to bytes
   */
  private byte[] getBytes(Object value, CarbonColumn col) {
    byte[] indexValue;
    if (col.isMeasure()) {
      if (col.getDataType().equals(DataTypes.BOOLEAN)) {
        value = BooleanConvert.boolean2Byte((Boolean)value);
      }
      indexValue = CarbonUtil.getValueAsBytes(col.getDataType(), value);
    } else {
      if (DataTypes.VARCHAR == col.getDataType()) {
        indexValue = BloomFilterUtil.getRawBytesForVarchar((byte[]) value);
      } else if (DataTypeUtil.isPrimitiveColumn(col.getDataType())) {
        indexValue = CarbonUtil.getValueAsBytes(col.getDataType(), value);
      } else {
        indexValue = BloomFilterUtil.getRawBytes((byte[]) value);
      }
    }
    if (indexValue.length == 0) {
      indexValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
    }
    return indexValue;
  }

}
