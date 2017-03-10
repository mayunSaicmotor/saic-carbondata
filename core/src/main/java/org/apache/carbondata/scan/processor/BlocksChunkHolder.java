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
package org.apache.carbondata.scan.processor;

import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.scan.collector.impl.DictionaryBasedResultCollectorForSort;
import org.apache.carbondata.scan.collector.impl.RawBasedResultCollector;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.model.QueryDimension;
import org.apache.carbondata.scan.model.SortOrderType;
import org.apache.carbondata.scan.result.AbstractScannedResult;
import org.apache.carbondata.scan.scanner.BlockletScanner;

/**
 * Block chunk holder which will hold the dimension and
 * measure chunk
 */
public class BlocksChunkHolder {

	//TODO
	  private BlockletScanner blockletScanner;
	  private QueryDimension singleSortDimesion;
	  private int limit = 0;
	  private int nodeSize;
	  private byte[] maxValueForSortKey;
	  private byte[] minValueForSortKey;
	  private long nodeNumber;
	  private String blockletIndentifyPath;
	  private BlockExecutionInfo blockExecutionInfo;
	  
	  
  public String getBlockletIndentifyPath() {
		return blockletIndentifyPath;
	}

	public void setBlockletIndentifyPath(String blockletIndentifyPath) {
		this.blockletIndentifyPath = blockletIndentifyPath;
	}

public BlockExecutionInfo getBlockExecutionInfo() {
		return blockExecutionInfo;
	}

	public void setBlockExecutionInfo(BlockExecutionInfo blockExecutionInfo) {
		this.blockExecutionInfo = blockExecutionInfo;
	}

public int getNodeSize() {
		return nodeSize;
	}

	public void setNodeSize(int nodeSize) {
		this.nodeSize = nodeSize;
	}

public long getNodeNumber() {
		return nodeNumber;
	}

	public void setNodeNumber(long nodeNumber) {
		this.nodeNumber = nodeNumber;
	}

public byte[] getMaxValueForSortKey() {
		return maxValueForSortKey;
	}

	public void setMaxValueForSortKey(byte[] maxValueForSortKey) {
		this.maxValueForSortKey = maxValueForSortKey;
	}

	public byte[] getMinValueForSortKey() {
		return minValueForSortKey;
	}

	public void setMinValueForSortKey(byte[] minValueForSortKey) {
		this.minValueForSortKey = minValueForSortKey;
	}

public QueryDimension getSingleSortDimesion() {
		return singleSortDimesion;
	}

	public void setSingleSortDimesion(QueryDimension singleSortDimesion) {
		this.singleSortDimesion = singleSortDimesion;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

public BlockletScanner getBlockletScanner() {
		return blockletScanner;
	}

	public void setBlockletScanner(BlockletScanner blockletScanner) {
		this.blockletScanner = blockletScanner;
	}

	AbstractScannedResult scanBlocklet()
		      throws QueryExecutionException{
		
		return this.blockletScanner.scanBlocklet(this);
	}
	
	//TODO
	public AbstractScannedResult scanBlockletForSort(SortOrderType orderType)
		      throws QueryExecutionException{
		
		AbstractScannedResult result = this.blockletScanner.scanBlockletForSort(this, orderType);
	    if (blockExecutionInfo.isRawRecordDetailQuery()) {
	    	//TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO error
	      //this.scannerResultAggregator =
	          new RawBasedResultCollector(blockExecutionInfo);
	    } else {
	    	result.setScannerResultAggregator(new DictionaryBasedResultCollectorForSort(blockExecutionInfo));
	    }
		return result;
	}
	
/**
   * dimension column data chunk
   */
  private DimensionColumnDataChunk[] dimensionDataChunk;

  /**
   * measure column data chunk
   */
  private MeasureColumnDataChunk[] measureDataChunk;

  /**
   * file reader which will use to read the block from file
   */
  private FileHolder fileReader;

  /**
   * data block
   */
  private DataRefNode dataBlock;

  public BlocksChunkHolder(int numberOfDimensionBlock, int numberOfMeasureBlock) {
    dimensionDataChunk = new DimensionColumnDataChunk[numberOfDimensionBlock];
    measureDataChunk = new MeasureColumnDataChunk[numberOfMeasureBlock];
  }
  
/*  public BlocksChunkHolder(int numberOfDimensionBlock, int numberOfMeasureBlock, QueryDimension singleSortDimesion, int limit) {
	    this.dimensionDataChunk = new DimensionColumnDataChunk[numberOfDimensionBlock];
	    this.measureDataChunk = new MeasureColumnDataChunk[numberOfMeasureBlock];
	    this.singleSortDimesion = singleSortDimesion;
	    this.limit = limit;
	  }*/
  

  /**
   * @return the dimensionDataChunk
   */
  public DimensionColumnDataChunk[] getDimensionDataChunk() {
    return dimensionDataChunk;
  }

  /**
   * @param dimensionDataChunk the dimensionDataChunk to set
   */
  public void setDimensionDataChunk(DimensionColumnDataChunk[] dimensionDataChunk) {
    this.dimensionDataChunk = dimensionDataChunk;
  }

  /**
   * @return the measureDataChunk
   */
  public MeasureColumnDataChunk[] getMeasureDataChunk() {
    return measureDataChunk;
  }

  /**
   * @param measureDataChunk the measureDataChunk to set
   */
  public void setMeasureDataChunk(MeasureColumnDataChunk[] measureDataChunk) {
    this.measureDataChunk = measureDataChunk;
  }

  /**
   * @return the fileReader
   */
  public FileHolder getFileReader() {
    return fileReader;
  }

  /**
   * @param fileReader the fileReader to set
   */
  public void setFileReader(FileHolder fileReader) {
    this.fileReader = fileReader;
  }

  /**
   * @return the dataBlock
   */
  public DataRefNode getDataBlock() {
    return dataBlock;
  }

  /**
   * @param dataBlock the dataBlock to set
   */
  public void setDataBlock(DataRefNode dataBlock) {
    this.dataBlock = dataBlock;
  }

  /***
   * To reset the measure chunk and dimension chunk
   * array
   */
  public void reset() {
    for (int i = 0; i < measureDataChunk.length; i++) {
      this.measureDataChunk[i] = null;
    }
    for (int i = 0; i < dimensionDataChunk.length; i++) {
      this.dimensionDataChunk[i] = null;
    }
  }
}
