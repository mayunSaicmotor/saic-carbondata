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

import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.result.AbstractScannedResult;

/**
 * Result provide class for non filter query
 * In case of no filter query we need to return
 * complete data
 */
public class NonFilterQueryScannedResultForSort extends AbstractScannedResult {

  public NonFilterQueryScannedResultForSort(BlockExecutionInfo blockExecutionInfo) {
    super(blockExecutionInfo);
  }

  /**
   * As this class will be a flyweight object so
   * for one block all the blocklet scanning will use same result object
   * in that case we need to reset the counter to zero so
   * for new result it will give the result from zero
   */
  @Override public void reset() {
    rowCounter = 0;
    currentRow = -1;
/*    if(this.descSortFlg){
    	currentRow = this.totalNumberOfRows;
    }else{
    	currentRow = -1;
    }*/

  }
  
  /**
   * @return dictionary key array for all the dictionary dimension selected in
   * query
   */
  @Override public byte[] getDictionaryKeyArray() {
   ++currentRow;
    
	//incrementCurrentRowBySortType();
    return getDictionaryKeyArray(currentRow);
  }

  /**
   * @return dictionary key integer array for all the dictionary dimension
   * selected in query
   */
  //TODO
  @Override public int[] getDictionaryKeyIntegerArray() {
  return null;
  }
  
  /**
   * @return dictionary key integer array for all the dictionary dimension
   * selected in query
   */
/*  @Override public int[] getDictionaryKeyIntegerArrayHasLimitKey(Integer limitKey) {
		if(limitKey == null){
			   if(descSortFlg){
			   
			   		this.limitKey = Integer.MIN_VALUE;
			   }else{
			   
			   		this.limitKey = Integer.MAX_VALUE;
			   }
			   
			
		} else {
			
			this.limitKey = limitKey;
		}
    //++currentRow;
	incrementCurrentRowBySortType();
    return getDictionaryKeyIntegerArray(currentRow);
  }*/
  

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  @Override public byte[][] getComplexTypeKeyArray() {
    return getComplexTypeKeyArray(currentRow);
  }

  /**
   * Below method will be used to get the no dictionary key array for all the
   * no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override public byte[][] getNoDictionaryKeyArray() {
    return getNoDictionaryKeyArray(currentRow);
  }

  /**
   * Below method will be used to get the no dictionary key
   * string array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override public String[] getNoDictionaryKeyStringArray() {
	  //TODO
	  return getNoDictionaryKeyStringArray(this.currentLogicRowId);
    //return getNoDictionaryKeyStringArray(currentRow);
  }

  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  @Override public int getCurrenrRowId() {
    return this.currentLogicRowId;
	  //return this.currentRow;
  }

  /**
   * Return the dimension data based on dimension ordinal
   *
   * @param dimensionOrdinal dimension ordinal
   * @return dimension data
   */
  @Override public byte[] getDimensionKey(int dimensionOrdinal) {
    return getDimensionData(dimensionOrdinal, currentRow);
  }

  /**
   * Below method will be used to to check whether measure value is null or
   * for a measure
   *
   * @param ordinal measure ordinal
   * @return is null or not
   */
  @Override public boolean isNullMeasureValue(int ordinal) {
    return isNullMeasureValue(ordinal, currentRow);
  }

  /**
   * Below method will be used to get the measure value for measure of long
   * data type
   *
   * @param ordinal measure ordinal
   * @return long value of measure
   */
  @Override public long getLongMeasureValue(int ordinal) {
    return getLongMeasureValue(ordinal, currentRow);
  }

  /**
   * Below method will be used to get the value of measure of double type
   *
   * @param ordinal measure ordinal
   * @return measure value
   */
  @Override public double getDoubleMeasureValue(int ordinal) {
    return getDoubleMeasureValue(ordinal, currentRow);
  }

  /**
   * Below method will be used to get the data of big decimal type of a
   * measure
   *
   * @param ordinal measure ordinal
   * @return measure value
   */
  @Override public BigDecimal getBigDecimalMeasureValue(int ordinal) {
    return getBigDecimalMeasureValue(ordinal, currentRow);
  }

}
