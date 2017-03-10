/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.scan.result.impl;

import java.math.BigDecimal;

import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.scan.result.AbstractScannedResult;

/**
 * Result provider class in case of filter query
 * In case of filter query data will be send
 * based on filtered row index
 */
public class FilterQueryScannedResultForSort extends AbstractScannedResult {

  public FilterQueryScannedResultForSort(BlockExecutionInfo tableBlockExecutionInfos) {
    super(tableBlockExecutionInfos);
  }

  /**
   * @return dictionary key integer array for all the dictionary dimension
   * selected in query
   */
/*  @Override public int[] getDictionaryKeyIntegerArrayHasLimitKey(Integer limitKey) {
		if(limitKey == null){
			this.limitKey = Integer.MAX_VALUE;
		} else {
			
			this.limitKey = limitKey;
		}
    //++currentRow;
		incrementCurrentRowBySortType();
    return getDictionaryKeyIntegerArray(currentRow);
  }*/
  
  
  /**
   * @return dictionary key array for all the dictionary dimension
   * selected in query
   */
  @Override public byte[] getDictionaryKeyArray() {
    ++currentRow;
	 // incrementCurrentRowBySortType();
    return getDictionaryKeyArray(rowMapping[currentRow]);
  }

  /**
   * @return dictionary key integer array for all the dictionary dimension
   * selected in query
   */
  @Override public int[] getDictionaryKeyIntegerArray() {
    ++currentRow;
    //incrementCurrentRowBySortType();
    return getDictionaryKeyIntegerArray(rowMapping[currentRow]);
  }

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  @Override public byte[][] getComplexTypeKeyArray() {
    return getComplexTypeKeyArray(rowMapping[currentRow]);
  }

  /**
   * Below method will be used to get the no dictionary key
   * array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override public byte[][] getNoDictionaryKeyArray() {
    return getNoDictionaryKeyArray(rowMapping[currentRow]);
  }

  /**
   * Below method will be used to get the no dictionary key
   * string array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override public String[] getNoDictionaryKeyStringArray() {
    return getNoDictionaryKeyStringArray(this.currentLogicRowId);
  }

  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  @Override public int getCurrenrRowId() {
    //return rowMapping[currentRow];
	  return this.currentLogicRowId;
  }

  /**
   * Return the dimension data based on dimension ordinal
   *
   * @param dimensionOrdinal dimension ordinal
   * @return dimension data
   */
  @Override public byte[] getDimensionKey(int dimensionOrdinal) {
    return getDimensionData(dimensionOrdinal, rowMapping[currentRow]);
  }

  /**
   * Below method will be used to to check whether measure value
   * is null or for a measure
   *
   * @param ordinal measure ordinal
   * @return is null or not
   */
  @Override public boolean isNullMeasureValue(int ordinal) {
    return isNullMeasureValue(ordinal, rowMapping[currentRow]);
  }

  /**
   * Below method will be used to get the measure value for measure
   * of long data type
   *
   * @param ordinal measure ordinal
   * @return long value of measure
   */
  @Override public long getLongMeasureValue(int ordinal) {
    return getLongMeasureValue(ordinal, rowMapping[currentRow]);
  }

  /**
   * Below method will be used to get the value of measure of double
   * type
   *
   * @param ordinal measure ordinal
   * @return measure value
   */
  @Override public double getDoubleMeasureValue(int ordinal) {
    return getDoubleMeasureValue(ordinal, rowMapping[currentRow]);
  }

  /**
   * Below method will be used to get the data of big decimal type
   * of a measure
   *
   * @param ordinal measure ordinal
   * @return measure value
   */
  @Override public BigDecimal getBigDecimalMeasureValue(int ordinal) {
    return getBigDecimalMeasureValue(ordinal, rowMapping[currentRow]);
  }

  
  //TODO FOR  FILTER query
  @Override  public void loadOtherColunmsData()
  {
	  if(this.isLoadDataDelay()){
			
			BlocksChunkHolder blocksChunkHolder = this.getBlocksChunkHolder();
			BlockExecutionInfo blockExecutionInfo= blocksChunkHolder.getBlockExecutionInfo();

			
			
		    int[] allSelectedDimensionBlocksIndexes =
		            blockExecutionInfo.getAllSelectedDimensionBlocksIndexes();
		        DimensionColumnDataChunk[] dimensionColumnDataChunk = this.getDimensionChunks();
		        // read dimension chunk blocks from file which is not present
		        for (int i = 0; i < allSelectedDimensionBlocksIndexes.length; i++) {
		          if (null == blocksChunkHolder.getDimensionDataChunk()[allSelectedDimensionBlocksIndexes[i]]) {
		            dimensionColumnDataChunk[allSelectedDimensionBlocksIndexes[i]] =
		                blocksChunkHolder.getDataBlock()
		                    .getDimensionChunk(blocksChunkHolder.getFileReader(), allSelectedDimensionBlocksIndexes[i], blocksChunkHolder.getLimit(), this.getMaxLogicalRowIdByLimit()+1, this.descSortFlg);
		          } /*else {
		            dimensionColumnDataChunk[allSelectedDimensionBlocksIndexes[i]] =
		                blocksChunkHolder.getDimensionDataChunk()[allSelectedDimensionBlocksIndexes[i]];
		          }*/
		        }
		        
		        MeasureColumnDataChunk[] measureColumnDataChunk = blocksChunkHolder.getMeasureDataChunk();
		                //new MeasureColumnDataChunk[blockExecutionInfo.getTotalNumberOfMeasureBlock()];
		            int[] allSelectedMeasureBlocksIndexes = blockExecutionInfo.getAllSelectedMeasureBlocksIndexes();

		            // read the measure chunk blocks which is not present
		            for (int i = 0; i < allSelectedMeasureBlocksIndexes.length; i++) {

		              if (null == blocksChunkHolder.getMeasureDataChunk()[allSelectedMeasureBlocksIndexes[i]]) {
		                measureColumnDataChunk[allSelectedMeasureBlocksIndexes[i]] =
		                    blocksChunkHolder.getDataBlock()
		                        .getMeassureChunk(blocksChunkHolder.getFileReader(), allSelectedMeasureBlocksIndexes[i], this.getMaxLogicalRowIdByLimit()+1);
		              }/* else {
		                measureColumnDataChunk[allSelectedMeasureBlocksIndexes[i]] =
		                    blocksChunkHolder.getMeasureDataChunk()[allSelectedMeasureBlocksIndexes[i]];
		              }*/
		            }

/*							DimensionColumnDataChunk[] dimensionColumnDataChunks = blocksChunkHolder.getDataBlock()
					.getDimensionChunksForSort(blocksChunkHolder.getFileReader(),
							blockExecutionInfo.getAllSortDimensionBlocksIndexes(), blockExecutionInfo.getLimit());*/
			this.setDimensionChunks(dimensionColumnDataChunk); 
			this.setMeasureChunks(measureColumnDataChunk);
			this.resetLoadDataDelay();
		}  
  
  }
}
