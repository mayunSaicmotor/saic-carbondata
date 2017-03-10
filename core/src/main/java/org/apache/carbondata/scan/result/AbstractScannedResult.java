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
package org.apache.carbondata.scan.result;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.scan.collector.ScannedResultCollector;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.executor.infos.KeyStructureInfo;
import org.apache.carbondata.scan.filter.GenericQueryType;
import org.apache.carbondata.scan.model.SortOrderType;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;

/**
 * Scanned result class which will store and provide the result on request
 */
public abstract class AbstractScannedResult {

	private SortOrderType sortType = SortOrderType.NONE;
	private BlocksChunkHolder blocksChunkHolder;
	
	private boolean loadDataDelay=false;
	
	private boolean filterQueryFlg=false;
	
	private boolean sortByDictionaryDimensionFlg=false;
	private boolean sortByNoDictionaryDimensionFlg=false;
	
	public boolean isSortByNoDictionaryDimensionFlg() {
		return sortByNoDictionaryDimensionFlg;
	}

	public void setSortByNoDictionaryDimensionFlg(boolean sortByNoDictionaryDimensionFlg) {
		this.sortByNoDictionaryDimensionFlg = sortByNoDictionaryDimensionFlg;
	}

	public boolean isSortByDictionaryDimensionFlg() {
		return sortByDictionaryDimensionFlg;
	}

	public void setSortByDictionaryDimensionFlg(boolean sortByDictionaryDimensionFlg) {
		this.sortByDictionaryDimensionFlg = sortByDictionaryDimensionFlg;
	}

	public boolean isFilterQueryFlg() {
		return filterQueryFlg;
	}

	public void setFilterQueryFlg(boolean filterQueryFlg) {
		this.filterQueryFlg = filterQueryFlg;
	}

	//from 0 to n-1
	private int maxLogicalRowIdByLimit=-1;	
	
	public int getMaxLogicalRowIdByLimit() {
		
		//System.out.println("get maxLogicalRowIdByLimit: "+ maxLogicalRowIdByLimit);
		if(maxLogicalRowIdByLimit>=0){
			return maxLogicalRowIdByLimit;
		}
		return this.totalNumberOfRows;
	}

	public void setMaxLogicalRowIdByLimit(int maxLogicalRowIdByLimit) {
		this.maxLogicalRowIdByLimit = maxLogicalRowIdByLimit;
	}

	public boolean isLoadDataDelay() {
		return loadDataDelay;
	}

	public void setLoadDataDelay(boolean loadDataDelay) {
		this.loadDataDelay = loadDataDelay;
	}
	
	public void resetLoadDataDelay() {
		this.loadDataDelay = false;
	}

	public BlocksChunkHolder getBlocksChunkHolder() {
		return blocksChunkHolder;
	}

	public void setBlocksChunkHolder(BlocksChunkHolder blocksChunkHolder) {
		this.blocksChunkHolder = blocksChunkHolder;
	}

	public SortOrderType getSortType() {
		return sortType;
	}

	public void setSortType(SortOrderType sortType) {
		this.sortType = sortType;
	}

	 private Long nodeNumber;
	 private String blockletIndentifyPath;

public String getBlockletIndentifyPath() {
		return blockletIndentifyPath;
	}

	public void setBlockletIndentifyPath(String blockletIndentifyPath) {
		this.blockletIndentifyPath = blockletIndentifyPath;
	}

public Long getNodeNumber() {
		return nodeNumber;
	}

	public void setNodeNumber(Long nodeNumber) {
		this.nodeNumber = nodeNumber;
	}

private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractScannedResult.class.getName());

  
  
  
/**
   * current row number
   */
  protected int currentRow = -1;
  
  public int getCurrentRow() {
	return currentRow;
}

public void setCurrentRow(int currentRow) {
	this.currentRow = currentRow;
}

protected boolean descSortFlg = false;
  
  protected int currentLogicRowId = -1;
  
  public int getCurrentLogicRowId() {
	return currentLogicRowId;
}

public void setCurrentLogicRowId(int currentLogicRowId) {
	this.currentLogicRowId = currentLogicRowId;
}

/**
   * row mapping indexes
   */
  protected int[] rowMapping = null;
  
  public int[] getRowMapping() {
	return rowMapping;
}

public void setRowMapping(int[] rowMapping) {
	this.rowMapping = rowMapping;
}

protected String stopKey = null;
  
  
  public String getStopKey() {
	return stopKey;
}

public void setStopKey(String stopKey) {
	if(stopKey == null){
		stopKey = "";
	}
	this.stopKey = stopKey;
}

int sortDimentionIndexForSelect;

protected String currentSortDimentionKey = null;

protected boolean currentSortDimentionKeyChgFlg = false;
  
  public boolean isCurrentSortDimentionKeyChgFlg() {
	return currentSortDimentionKeyChgFlg;
}

public void setCurrentSortDimentionKeyChgFlg(boolean currentSortDimentionKeyChgFlg) {
	this.currentSortDimentionKeyChgFlg = currentSortDimentionKeyChgFlg;
}

public String getCurrentSortDimentionKey() {
	return currentSortDimentionKey;
}

public void setCurrentSortDimentionKey(String currentSortDimentionKey) {
	this.currentSortDimentionKey = currentSortDimentionKey;
}



protected int sortSingleDimensionBlocksIndex = -1;
protected DimensionColumnDataChunk sortDimention = null;
  protected boolean pauseProcessForSortFlg = false;
  protected int[] pausedCompleteKey = null;
  
  protected String[] pausedNoDictionaryKeys = null;
  
  public boolean isPauseProcessForSortFlg() {
	return pauseProcessForSortFlg;
}
  
  public void resetPauseProcessForSortFlg() {
	pauseProcessForSortFlg = false;
}
  

public void setPauseProcessForSortFlg(boolean pauseProcessForSortFlg) {
	this.pauseProcessForSortFlg = pauseProcessForSortFlg;
}

public void resetPauseData() {
	this.pausedCompleteKey = null;
	this.pausedNoDictionaryKeys = null;
	this.pauseProcessForSortFlg =false;
	//rowCounter++;
}

public String[] getPausedNoDictionaryKeys() {
	return pausedNoDictionaryKeys;
}

public void setPausedNoDictionaryKeys(String[] pausedNoDictionaryKeys) {
	this.pausedNoDictionaryKeys = pausedNoDictionaryKeys;
}

public int[] getPausedCompleteKey() {

	return pausedCompleteKey;
}

public void setPausedCompleteKey(int[] pausedCompleteKey) {
	this.pausedCompleteKey = pausedCompleteKey;
}



//TODO
  protected ScannedResultCollector scannerResultAggregator;
  
  
  
  public ScannedResultCollector getScannerResultAggregator() {
	return scannerResultAggregator;
}
  
  public List<Object[]> collectSortedData(int batchSize, String stopKey){
//	  System.out.println("nextSortDimentionKey: " + stopKey);
//	  if("name999989".equals(stopKey)){
//		  System.out.println("nextSortDimentionKey: " + stopKey); 
//	  }
	  this.loadOtherColunmsData();
	  return this.scannerResultAggregator.collectSortedData(this, batchSize, stopKey);
  }
  

public void setScannerResultAggregator(ScannedResultCollector scannerResultAggregator) {
	this.scannerResultAggregator = scannerResultAggregator;
}



  protected int[] baseSortDimentionInvertedIndexes = null;
  protected int currentPhysicalIndexForFilter = 0;
  
  protected int currentPhysicalRowIdForSortDimension = 0;
  
  /**
   * sorted dimension indexes
   */
private int[] allSortDimensionBlocksIndexes;

public int[] getAllSortDimensionBlocksIndexes() {
	return allSortDimensionBlocksIndexes;
}

public void setAllSortDimensionBlocksIndexes() {
	//this.allSortDimensionBlocksIndexes = allSortDimensionBlocksIndexes;
	
	//only consider one sort dimension currently
	if (allSortDimensionBlocksIndexes != null && allSortDimensionBlocksIndexes.length > 0) {
		sortSingleDimensionBlocksIndex = allSortDimensionBlocksIndexes[0];
		sortDimention = dataChunks[sortSingleDimensionBlocksIndex];
		baseSortDimentionInvertedIndexes = dataChunks[sortSingleDimensionBlocksIndex].getAttributes().getInvertedIndexes();
		
		for(int i =0 ; i< this.dictionaryColumnBlockIndexes.length; i++){
			if(dictionaryColumnBlockIndexes[i] == sortSingleDimensionBlocksIndex){
				sortByDictionaryDimensionFlg=true;
				break;
			}
		}
		
		if(!sortByDictionaryDimensionFlg){
			
			for(int i =0 ; i< this.noDictionaryColumnBlockIndexes.length; i++){
				if(noDictionaryColumnBlockIndexes[i] == sortSingleDimensionBlocksIndex){
					sortByNoDictionaryDimensionFlg=true;
					break;
				}
			}
		}

	}
}

/*public void setCurrentDictionaryKeyForSortDimention() {


	int[] keyArr = new int[1];
	sortDimention.fillConvertedChunkData(currentRow+1, 0, keyArr,
            columnGroupKeyStructureInfo.get(sortSingleDimensionBlocksIndex));
	
	this.currentSortDimentionKey = keyArr[0];
}*/

public boolean hasNextCurrentDictionaryKeyForSortDimention() {
    return rowCounter < this.totalNumberOfRows;
  }


public void initCurrentKeyForSortDimention(SortOrderType sortType) {

	this.sortType = sortType;
	if(SortOrderType.DSC.equals(sortType)){
		this.descSortFlg = true;
		//currentRow = this.totalNumberOfRows;
		
		if(baseSortDimentionInvertedIndexes!=null){
			
			currentPhysicalIndexForFilter = baseSortDimentionInvertedIndexes.length -1;
		}else{
			
			currentPhysicalIndexForFilter = this.blocksChunkHolder.getNodeSize()-1;
		}
		
		
	}
	nextCurrentKeyForSortDimention();
}

public void nextCurrentKeyForSortDimention() {

	
	int[] keyArr = new int[1];

/*	int tmpRowCounter = rowCounter;k
	if(descSortFlg){
		tmpRowCounter=this.totalNumberOfRows-1;
		
	}*/
	//sortDimention.fillConvertedChunkData(rowCounter, 0, keyArr, columnGroupKeyStructureInfo.get(sortSingleDimensionBlocksIndex));
	if(this.sortByDictionaryDimensionFlg){
		sortDimention.fillConvertedChunkData(getStartRowIndex(), 0, keyArr, columnGroupKeyStructureInfo.get(sortSingleDimensionBlocksIndex));
		this.currentSortDimentionKey = Integer.toString(keyArr[0]);
	}else if(this.sortByNoDictionaryDimensionFlg){
		
		//sortDimention.fillConvertedChunkData(getStartRowIndex(), 0, keyArr, columnGroupKeyStructureInfo.get(sortSingleDimensionBlocksIndex));	
		this.currentSortDimentionKey = new String(sortDimention.getChunkDataByPhysicalRowId(getStartRowIndex()));
		
		
	}else{
			
			//TODO
			this.currentSortDimentionKey = new String(sortDimention.getChunkData(getStartRowIndex()));
			
			
		}

}

private int getStartRowIndex() {
	
	/*if(this.rowMapping != null && this.rowMapping.length > 0 && baseSortDimentionInvertedIndexes!=null){
		for(int index = currentFilterPhysicalIndex; index < baseSortDimentionInvertedIndexes.length; index++){
			// this.currentFilterPhysicalIndex = Arrays.binarySearch(this.rowMapping, index);
			if(Arrays.binarySearch(this.rowMapping, baseSortDimentionInvertedIndexes[index]) >= 0){
				//System.out.println(" filtered index: " + baseSortDimentionInvertedIndexes[index]);
				this.currentFilterPhysicalIndex = index+1;
				rowId = index;
				this.currentInvertedRowIdForMessure=baseSortDimentionInvertedIndexes[rowId];	
				break;
			}
			
		}
	}*/
	//return descSortFlg?sortDimention.getTotalRowNumber()-1:rowCounter;
	
	return descSortFlg?sortDimention.getTotalRowNumber()-1:rowCounter;
	//return rowCounter;
}

  
  /**
   * key size of the fixed length column
   */
  private int fixedLengthKeySize;
  /**
   * total number of rows
   */
  protected int totalNumberOfRows;
  /**
   * to keep track of number of rows process
   */
  protected int rowCounter;
  /**
   * dimension column data chunk
   */
  protected DimensionColumnDataChunk[] dataChunks;
  /**
   * measure column data chunk
   */
  protected MeasureColumnDataChunk[] measureDataChunks;
  /**
   * dictionary column block index in file
   */
  private int[] dictionaryColumnBlockIndexes;

  /**
   * no dictionary column block index in file
   */
  private int[] noDictionaryColumnBlockIndexes;

  /**
   * column group to is key structure info
   * which will be used to get the key from the complete
   * column group key
   * For example if only one dimension of the column group is selected
   * then from complete column group key it will be used to mask the key and
   * get the particular column key
   */
  private Map<Integer, KeyStructureInfo> columnGroupKeyStructureInfo;

  /**
   *
   */
  private Map<Integer, GenericQueryType> complexParentIndexToQueryMap;

  private int totalDimensionsSize;

  /**
   * parent block indexes
   */
  private int[] complexParentBlockIndexes;

  public AbstractScannedResult(BlockExecutionInfo blockExecutionInfo) {
    this.fixedLengthKeySize = blockExecutionInfo.getFixedLengthKeySize();
    this.noDictionaryColumnBlockIndexes = blockExecutionInfo.getNoDictionaryBlockIndexes();
    this.dictionaryColumnBlockIndexes = blockExecutionInfo.getDictionaryColumnBlockIndex();
    this.columnGroupKeyStructureInfo = blockExecutionInfo.getColumnGroupToKeyStructureInfo();
    this.complexParentIndexToQueryMap = blockExecutionInfo.getComlexDimensionInfoMap();
    this.complexParentBlockIndexes = blockExecutionInfo.getComplexColumnParentBlockIndexes();
    this.totalDimensionsSize = blockExecutionInfo.getQueryDimensions().length;
    //this.setAllSortDimensionBlocksIndexes(blockExecutionInfo.getAllSortDimensionBlocksIndexes());
    this.allSortDimensionBlocksIndexes = blockExecutionInfo.getAllSortDimensionBlocksIndexes();
    
    
  }

  /**
   * Below method will be used to set the dimension chunks
   * which will be used to create a row
   *
   * @param dataChunks dimension chunks used in query
   */
  public void setDimensionChunks(DimensionColumnDataChunk[] dataChunks) {
    this.dataChunks = dataChunks;
  }
  
  /**
   * Below method will be used to set the dimension chunks
   * which will be used to create a row
   *
   * @param dataChunks dimension chunks used in query
   */
  public DimensionColumnDataChunk[]  getDimensionChunks() {
    return dataChunks;
  }
  

  /**
   * Below method will be used to set the measure column chunks
   *
   * @param measureDataChunks measure data chunks
   */
  public void setMeasureChunks(MeasureColumnDataChunk[] measureDataChunks) {
    this.measureDataChunks = measureDataChunks;
  }

  /**
   * Below method will be used to get the chunk based in measure ordinal
   *
   * @param ordinal measure ordinal
   * @return measure column chunk
   */
  public MeasureColumnDataChunk getMeasureChunk(int ordinal) {
    return measureDataChunks[ordinal];
  }

  /**
   * Below method will be used to get the key for all the dictionary dimensions
   * which is present in the query
   *
   * @param rowId row id selected after scanning
   * @return return the dictionary key
   */
  protected byte[] getDictionaryKeyArray(int rowId) {
    byte[] completeKey = new byte[fixedLengthKeySize];
    int offset = 0;
    for (int i = 0; i < this.dictionaryColumnBlockIndexes.length; i++) {
      offset += dataChunks[dictionaryColumnBlockIndexes[i]]
          .fillChunkData(completeKey, offset, rowId,
              columnGroupKeyStructureInfo.get(dictionaryColumnBlockIndexes[i]));
    }
    rowCounter++;
    return completeKey;
  }

  /**
   * Below method will be used to get the key for all the dictionary dimensions
   * in integer array format which is present in the query
   *
   * @param rowId row id selected after scanning
   * @return return the dictionary key
   */
  protected int[] getDictionaryKeyIntegerArray(int rowId) {
	  
  
    int[] completeKey = new int[totalDimensionsSize];
    int column = 0;
	
	caculateCurrentRowId(rowId);
	//TODO
	int tmpPhysicalRowId = currentPhysicalRowIdForSortDimension;

	int[] tmpNextInvertedIndexes =  null;
    for (int i = 0; i < this.dictionaryColumnBlockIndexes.length; i++) {
    	
    	//boolean sortDimentionFlg = false;

    	DimensionChunkAttributes chunkAttributes = dataChunks[dictionaryColumnBlockIndexes[i]].getAttributes();
    	
    	// for no sort query 
			if (sortSingleDimensionBlocksIndex < 0) {
				//and the dimension which has inverted index
				if (chunkAttributes.getInvertedIndexes() != null) {
					tmpPhysicalRowId = chunkAttributes.getInvertedIndexesReverse()[currentLogicRowId];
				}
				// tmpPhysicalRowId =
				// chunkAttributes.getInvertedIndexesReverse()[currentLogicRowId];
				// invertedRowId =rowId;
			} else {

				// for dimension which has no inverted index and is not the
				// specified sort dimension in sql
				if (sortSingleDimensionBlocksIndex != dictionaryColumnBlockIndexes[i]) {

					tmpNextInvertedIndexes = dataChunks[dictionaryColumnBlockIndexes[i]].getAttributes()
							.getInvertedIndexesReverse();
					// System.out.println("rowId: " + rowId);
					if (tmpNextInvertedIndexes != null) {
						// if(baseSortDimentionInvertedIndexes != null ){

						// int tmpIndex
						// =baseSortDimentionInvertedIndexes[rowId];
						tmpPhysicalRowId = tmpNextInvertedIndexes[currentLogicRowId];
						// }else{
						// tmpPhysicalRowId = tmpNextInvertedIndexes[rowId];
						// }

					// for the specified sort dimension in sql
					} else {

						if (baseSortDimentionInvertedIndexes == null) {
							tmpPhysicalRowId = currentPhysicalRowIdForSortDimension;
						} else {
							tmpPhysicalRowId = baseSortDimentionInvertedIndexes[currentPhysicalRowIdForSortDimension];
						}
						// rowId =
						// chunkAttributes.getInvertedIndexesReverse()[chunkAttributes.getInvertedIndexes()[rowId]];
					}

					// for sort dimention, use the rowid directly
				} else {

					tmpPhysicalRowId = currentPhysicalRowIdForSortDimension;
					sortDimentionIndexForSelect = i;
				}

			}
		
        column = dataChunks[dictionaryColumnBlockIndexes[i]].fillConvertedChunkData(tmpPhysicalRowId, column, completeKey,
              columnGroupKeyStructureInfo.get(dictionaryColumnBlockIndexes[i]));

    }
    
    // for dictionary dimension all key is number, but for no dictionary dimension, it is string
    if(sortByDictionaryDimensionFlg){
    	
		if (sortSingleDimensionBlocksIndex >= 0
				&& ((!this.descSortFlg && completeKey[sortDimentionIndexForSelect] > Integer.parseInt(stopKey))
						|| (this.descSortFlg && completeKey[sortDimentionIndexForSelect] < Integer.parseInt(stopKey)))) {
			pauseProcessCollectData(completeKey);
			//return completeKey;
		}
    }

    rowCounter++;
    //System.out.println("completeKey: "+completeKey.toString());
    return completeKey;
  }

public void caculateCurrentRowId(int rowId) {
	
	currentPhysicalRowIdForSortDimension = rowId;
	//for sort query push down sort
	if(sortSingleDimensionBlocksIndex >= 0){
		
		//baseSortDimentionInvertedIndexes = dataChunks[sortSingleDimensionBlocksIndex].getAttributes().getInvertedIndexes();
		
		if(baseSortDimentionInvertedIndexes != null){
			
			// set logical row id default value
			this.currentLogicRowId=baseSortDimentionInvertedIndexes[currentPhysicalRowIdForSortDimension];	
	
			// for filter query
			if(this.rowMapping != null && this.rowMapping.length > 0){
				
				caculateCurrentRowIdForFilterQuery();
			
			// for no filter query and baseSortDimentionInvertedIndexes != null
			}else{
				
				if(this.descSortFlg){				

					currentPhysicalRowIdForSortDimension = this.totalNumberOfRows- rowId -1;	
				}
				this.currentLogicRowId=baseSortDimentionInvertedIndexes[currentPhysicalRowIdForSortDimension];
			}
			
		//when baseSortDimentionInvertedIndexes = null	
		}else{
			
			// for filter query
			if(this.rowMapping != null && this.rowMapping.length > 0){
				
				caculateCurrentRowIdForNoInvertedIndexFilterQuery();
			}else{
				if(this.descSortFlg){
					
					currentPhysicalRowIdForSortDimension = this.totalNumberOfRows- rowId -1;
				}			
				this.currentLogicRowId=currentPhysicalRowIdForSortDimension;	
			}

		}
		
	//for no sort query
	}else{
		//TODO
		
		// for filter query
		if(this.rowMapping != null && this.rowMapping.length > 0){
			this.currentLogicRowId= rowMapping[rowId];
		}else{
			
			this.currentLogicRowId=currentPhysicalRowIdForSortDimension;
		}
	}
	//return rowId;
}

private void caculateCurrentRowIdForFilterQuery() {
	if(this.descSortFlg){
		for(int physicalRowId = currentPhysicalIndexForFilter; physicalRowId >=0; physicalRowId--){
			// this.currentFilterPhysicalIndex = Arrays.binarySearch(this.rowMapping, index);
			if(Arrays.binarySearch(this.rowMapping, baseSortDimentionInvertedIndexes[physicalRowId]) >= 0){
				//System.out.println(" filtered index: " + baseSortDimentionInvertedIndexes[index]);
				this.currentPhysicalIndexForFilter = physicalRowId-1;
				this.currentPhysicalRowIdForSortDimension = physicalRowId;
				this.currentLogicRowId=baseSortDimentionInvertedIndexes[currentPhysicalRowIdForSortDimension];	
				break;
			}
			
		}
	} else {
		for(int index = currentPhysicalIndexForFilter; index < baseSortDimentionInvertedIndexes.length; index++){
			// this.currentFilterPhysicalIndex = Arrays.binarySearch(this.rowMapping, index);
			if(Arrays.binarySearch(this.rowMapping, baseSortDimentionInvertedIndexes[index]) >= 0){
				//System.out.println(" filtered index: " + baseSortDimentionInvertedIndexes[index]);
				this.currentPhysicalIndexForFilter = index+1;
				this.currentPhysicalRowIdForSortDimension = index;
				this.currentLogicRowId=baseSortDimentionInvertedIndexes[currentPhysicalRowIdForSortDimension];	
				break;
			}
			
		}
	}
}

private void caculateCurrentRowIdForFilterQueryNew() {
	if(this.descSortFlg){
		for(int physicalRowId = currentPhysicalIndexForFilter; physicalRowId >=0; physicalRowId--){
			// this.currentFilterPhysicalIndex = Arrays.binarySearch(this.rowMapping, index);
			if(Arrays.binarySearch(this.rowMapping, baseSortDimentionInvertedIndexes[physicalRowId]) >= 0){
				//System.out.println(" filtered index: " + baseSortDimentionInvertedIndexes[index]);
				this.currentPhysicalIndexForFilter = physicalRowId-1;
				this.currentPhysicalRowIdForSortDimension = physicalRowId;
				this.currentLogicRowId=baseSortDimentionInvertedIndexes[currentPhysicalRowIdForSortDimension];	
				break;
			}
			
		}
	} else {
		for(int index = currentPhysicalIndexForFilter; index < baseSortDimentionInvertedIndexes.length; index++){
			// this.currentFilterPhysicalIndex = Arrays.binarySearch(this.rowMapping, index);
			if(Arrays.binarySearch(this.rowMapping, baseSortDimentionInvertedIndexes[index]) >= 0){
				//System.out.println(" filtered index: " + baseSortDimentionInvertedIndexes[index]);
				this.currentPhysicalIndexForFilter = index+1;
				this.currentPhysicalRowIdForSortDimension = index;
				this.currentLogicRowId=baseSortDimentionInvertedIndexes[currentPhysicalRowIdForSortDimension];	
				break;
			}
			
		}
	}
}


private void caculateCurrentRowIdForNoInvertedIndexFilterQuery() {
	if(this.descSortFlg){
		if(this.currentPhysicalIndexForFilter >= rowMapping.length){
			this.currentPhysicalIndexForFilter = this.rowMapping.length -1;
		}
		this.currentPhysicalRowIdForSortDimension = this.rowMapping[currentPhysicalIndexForFilter];
		this.currentLogicRowId=this.currentPhysicalRowIdForSortDimension;
		this.currentPhysicalIndexForFilter --;
	} else {
		this.currentPhysicalRowIdForSortDimension = this.rowMapping[currentPhysicalIndexForFilter];
		this.currentLogicRowId=this.currentPhysicalRowIdForSortDimension;
		this.currentPhysicalIndexForFilter ++;
	}
}

public void pauseProcessCollectData(int[] completeKey) {
	currentSortDimentionKey = Integer.toString(completeKey[sortDimentionIndexForSelect]);
	currentSortDimentionKeyChgFlg = true;
	pauseProcessForSortFlg = true;
	pausedCompleteKey = completeKey;
	//rowCounter++;
}

public void pauseProcessCollectData(String[] noDictonaryKeys) {
	currentSortDimentionKey = noDictonaryKeys[sortDimentionIndexForSelect];
	currentSortDimentionKeyChgFlg = true;
	pauseProcessForSortFlg = true;
	pausedNoDictionaryKeys = noDictonaryKeys;
	//rowCounter++;
}

  /**
   * Just increment the counter incase of query only on measures.
   */
  public void incrementCounter() {
    rowCounter ++;
    currentRow ++;
    caculateCurrentRowId(currentRow);

  }
  public void decrementRowCounter() {
	    rowCounter--;
  }
  
  public void incrementRowCounter() {
	    rowCounter++;
}

  /**
   * Just increment the counter incase of query only on measures.
   */
/*  public void incrementCurrentRowBySortType() {
	    if(descSortFlg){
	    	
	    	currentRow --;	
	    }else {
	    	
	    	currentRow ++;
	    }
    
  }*/
  
  /**
   * Below method will be used to get the dimension data based on dimension
   * ordinal and index
   *
   * @param dimOrdinal dimension ordinal present in the query
   * @param rowId      row index
   * @return dimension data based on row id
   */
  protected byte[] getDimensionData(int dimOrdinal, int rowId) {
    return dataChunks[dimOrdinal].getChunkData(rowId);
  }

  /**
   * Below method will be used to get the dimension key array
   * for all the no dictionary dimension present in the query
   *
   * @param rowId row number
   * @return no dictionary keys for all no dictionary dimension
   */
  protected byte[][] getNoDictionaryKeyArray(int rowId) {
    byte[][] noDictionaryColumnsKeys = new byte[noDictionaryColumnBlockIndexes.length][];
    int position = 0;
    for (int i = 0; i < this.noDictionaryColumnBlockIndexes.length; i++) {
      noDictionaryColumnsKeys[position++] =
          dataChunks[noDictionaryColumnBlockIndexes[i]].getChunkData(rowId);
    }
    return noDictionaryColumnsKeys;
  }

  /**
   * Below method will be used to get the dimension key array
   * for all the no dictionary dimension present in the query
   *
   * @param rowId row number
   * @return no dictionary keys for all no dictionary dimension
   */
  protected String[] getNoDictionaryKeyStringArray(int rowId) {
    String[] noDictionaryColumnsKeys = new String[noDictionaryColumnBlockIndexes.length];
    int position = 0;
    for (int i = 0; i < this.noDictionaryColumnBlockIndexes.length; i++) {
    	
    	//String noDictionryKey = new String(dataChunks[noDictionaryColumnBlockIndexes[i]].getChunkData(rowId));
    	// sort by this no dictionary 
    	if(noDictionaryColumnBlockIndexes[i] == this.sortSingleDimensionBlocksIndex && isSortByNoDictionaryDimensionFlg()){
    		

    		String noDictionryKey = new String(dataChunks[noDictionaryColumnBlockIndexes[i]].getChunkDataByPhysicalRowId(currentPhysicalRowIdForSortDimension));

    			sortDimentionIndexForSelect =i; 
    			noDictionaryColumnsKeys[position++] = noDictionryKey;
    	}else{
    		
    		String noDictionryKey = new String(dataChunks[noDictionaryColumnBlockIndexes[i]].getChunkData(rowId));
    		
//    		if("name1849981".equals(noDictionryKey)){
//    			for(int ii = 0 ; ii< dataChunks[noDictionaryColumnBlockIndexes[i]].getTotalRowNumber(); ii++){
//	    			if("name4650000".equals(new String(dataChunks[noDictionaryColumnBlockIndexes[i]].getChunkData(ii)))){
//	    				System.out.println(new String(dataChunks[noDictionaryColumnBlockIndexes[i]].getChunkData(ii)));
//	    			}
//    			} 
//		}
    		
    		noDictionaryColumnsKeys[position++] = noDictionryKey;
    	}
      //noDictionaryColumnsKeys[position++] = noDictionryKey;
    }

		if (this.sortByNoDictionaryDimensionFlg
				&& ((!this.descSortFlg && noDictionaryColumnsKeys[sortDimentionIndexForSelect].compareTo(stopKey) > 0)
						|| (this.descSortFlg
								&& noDictionaryColumnsKeys[sortDimentionIndexForSelect].compareTo(stopKey) < 0))) {

			pauseProcessCollectData(noDictionaryColumnsKeys);
			return noDictionaryColumnsKeys;
		}

		return noDictionaryColumnsKeys;
	}

  /**
   * Below method will be used to get the complex type keys array based
   * on row id for all the complex type dimension selected in query
   *
   * @param rowId row number
   * @return complex type key array for all the complex dimension selected in query
   */
  protected byte[][] getComplexTypeKeyArray(int rowId) {
    byte[][] complexTypeData = new byte[complexParentBlockIndexes.length][];
    for (int i = 0; i < complexTypeData.length; i++) {
      GenericQueryType genericQueryType =
          complexParentIndexToQueryMap.get(complexParentBlockIndexes[i]);
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream dataOutput = new DataOutputStream(byteStream);
      try {
        genericQueryType.parseBlocksAndReturnComplexColumnByteArray(dataChunks, rowId, dataOutput);
        complexTypeData[i] = byteStream.toByteArray();
      } catch (IOException e) {
        LOGGER.error(e);
      } finally {
        CarbonUtil.closeStreams(dataOutput);
        CarbonUtil.closeStreams(byteStream);
      }
    }
    return complexTypeData;
  }

  /**
   * @return return the total number of row after scanning
   */
  public int numberOfOutputRows() {
    return this.totalNumberOfRows;
  }

  /**
   * to check whether any more row is present in the result
   *
   * @return
   */
  public boolean hasNext() {
    return rowCounter < this.totalNumberOfRows;
  }
  
  public boolean hasNextForSort() {
	    //return (rowCounter < this.totalNumberOfRows) && pauseProcessForSortFlg;
	    return rowCounter < this.totalNumberOfRows;// || this.pauseProcessForSortFlg;//|| pauseProcessForSortFlg);// ||(rowCounter == this.totalNumberOfRows && this.pausedCompleteKey!=null);//|| this.pausedCompleteKey!=null;
	  }

  /**
   * As this class will be a flyweight object so
   * for one block all the blocklet scanning will use same result object
   * in that case we need to reset the counter to zero so
   * for new result it will give the result from zero
   */
  public void reset() {
    rowCounter = 0;
    currentRow = -1;
  }

  /**
   * @param totalNumberOfRows set total of number rows valid after scanning
   */
  public void setNumberOfRows(int totalNumberOfRows) {
    this.totalNumberOfRows = totalNumberOfRows;
  }

  /**
   * After applying filter it will return the  bit set with the valid row indexes
   * so below method will be used to set the row indexes
   *
   * @param indexes
   */
  public void setIndexes(int[] indexes) {
//		if(indexes.length == 4057){
//			System.out.println("rowMapping length: "+ indexes.length);
//		}
		
	  
    this.rowMapping = indexes;
  }
  
  

  /**
   * Below method will be used to check whether measure value is null or not
   *
   * @param ordinal  measure ordinal
   * @param rowIndex row number to be checked
   * @return whether it is null or not
   */
  protected boolean isNullMeasureValue(int ordinal, int rowIndex) {
    return measureDataChunks[ordinal].getNullValueIndexHolder().getBitSet().get(rowIndex);
  }

  /**
   * Below method will be used to get the measure value of
   * long type
   *
   * @param ordinal  measure ordinal
   * @param rowIndex row number of the measure value
   * @return measure value of long type
   */
  protected long getLongMeasureValue(int ordinal, int rowIndex) {
    return measureDataChunks[ordinal].getMeasureDataHolder().getReadableLongValueByIndex(rowIndex);
  }

  /**
   * Below method will be used to get the measure value of double type
   *
   * @param ordinal  measure ordinal
   * @param rowIndex row number
   * @return measure value of double type
   */
  protected double getDoubleMeasureValue(int ordinal, int rowIndex) {
    return measureDataChunks[ordinal].getMeasureDataHolder()
        .getReadableDoubleValueByIndex(rowIndex);
  }

  /**
   * Below method will be used to get the measure type of big decimal data type
   *
   * @param ordinal  ordinal of the of the measure
   * @param rowIndex row number
   * @return measure of big decimal type
   */
  protected BigDecimal getBigDecimalMeasureValue(int ordinal, int rowIndex) {
    return measureDataChunks[ordinal].getMeasureDataHolder()
        .getReadableBigDecimalValueByIndex(rowIndex);
  }

  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  public abstract int getCurrenrRowId();

  /**
   * @return dictionary key array for all the dictionary dimension
   * selected in query
   */
  public abstract byte[] getDictionaryKeyArray();

  /**
   * @return dictionary key array for all the dictionary dimension in integer array forat
   * selected in query
   */
  public abstract int[] getDictionaryKeyIntegerArray();
  
  /**
   * @return dictionary key array for all the dictionary dimension in integer array forat
   * selected in query
   */
  //TODO
  public int[] getDictionaryKeyIntegerArrayHasLimitKey(String stopKey) {
		if(stopKey == null){
			   if(descSortFlg){
				   if(this.sortByDictionaryDimensionFlg){
					   this.stopKey = String.valueOf(Integer.MIN_VALUE);
				   }else{
					   this.stopKey = CarbonCommonConstants.MIN_STR;
				   }
			   		
			   }else{
			   
				   if(this.sortByDictionaryDimensionFlg){
					   this.stopKey = String.valueOf(Integer.MAX_VALUE);
				   }else{
					   this.stopKey = CarbonCommonConstants.MAX_STR;
				   }
				   
			   		
			   }
			   
			
		} else {
			
			this.stopKey = stopKey;
		}
  ++currentRow;
	//incrementCurrentRowBySortType();
  return getDictionaryKeyIntegerArray(currentRow);
}
  /**
   * Return the dimension data based on dimension ordinal
   *
   * @param dimensionOrdinal dimension ordinal
   * @return dimension data
   */
  public abstract byte[] getDimensionKey(int dimensionOrdinal);

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  public abstract byte[][] getComplexTypeKeyArray();

  /**
   * Below method will be used to get the no dictionary key
   * array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  public abstract byte[][] getNoDictionaryKeyArray();

  /**
   * Below method will be used to get the no dictionary key
   * array in string array format for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  public abstract String[] getNoDictionaryKeyStringArray();

  /**
   * Below method will be used to to check whether measure value
   * is null or for a measure
   *
   * @param ordinal measure ordinal
   * @return is null or not
   */
  public abstract boolean isNullMeasureValue(int ordinal);

  /**
   * Below method will be used to get the measure value for measure
   * of long data type
   *
   * @param ordinal measure ordinal
   * @return long value of measure
   */
  public abstract long getLongMeasureValue(int ordinal);

  /**
   * Below method will be used to get the value of measure of double
   * type
   *
   * @param ordinal measure ordinal
   * @return measure value
   */
  public abstract double getDoubleMeasureValue(int ordinal);

  /**
   * Below method will be used to get the data of big decimal type
   * of a measure
   *
   * @param ordinal measure ordinal
   * @return measure value
   */
  public abstract BigDecimal getBigDecimalMeasureValue(int ordinal);
 
  //TODO FOR NO FILTER
  public void loadOtherColunmsData()
  {
	  if(this.isLoadDataDelay()){
			
			BlocksChunkHolder blocksChunkHolder = this.getBlocksChunkHolder();
			BlockExecutionInfo blockExecutionInfo= blocksChunkHolder.getBlockExecutionInfo();
			this.setMeasureChunks(blocksChunkHolder.getDataBlock()
			        .getMeasureChunks(blocksChunkHolder.getFileReader(),
			        		//TODO
			        		blockExecutionInfo.getAllSelectedMeasureBlocksIndexes(),this.getMaxLogicalRowIdByLimit()));
			
			DimensionColumnDataChunk[] dimensionColumnDataChunks =this.getDimensionChunks();
			int[] allSelectedDimensionBlocksIndexes = blockExecutionInfo.getAllSelectedDimensionBlocksIndexes();
			for(int i=0;i<allSelectedDimensionBlocksIndexes.length;i++) {
				
				//DimensionColumnDataChunk  d = dimensionColumnDataChunks[i];
				if(dimensionColumnDataChunks[allSelectedDimensionBlocksIndexes[i]] == null){
					//TODO
					dimensionColumnDataChunks[allSelectedDimensionBlocksIndexes[i]] = blocksChunkHolder.getDataBlock().getDimensionChunk(blocksChunkHolder.getFileReader(), allSelectedDimensionBlocksIndexes[i], blockExecutionInfo.getLimit());
					
				}
				
			}
/*							DimensionColumnDataChunk[] dimensionColumnDataChunks = blocksChunkHolder.getDataBlock()
					.getDimensionChunksForSort(blocksChunkHolder.getFileReader(),
							blockExecutionInfo.getAllSortDimensionBlocksIndexes(), blockExecutionInfo.getLimit());*/
			this.setDimensionChunks(dimensionColumnDataChunks);   
			this.resetLoadDataDelay();
		}  
  
  }
}
