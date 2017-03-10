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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsModel;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.scan.collector.ScannedResultCollector;
import org.apache.carbondata.scan.collector.impl.DictionaryBasedResultCollectorForSort;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.model.QueryDimension;
import org.apache.carbondata.scan.model.QueryModel;
import org.apache.carbondata.scan.result.AbstractScannedResult;
import org.apache.carbondata.scan.scanner.impl.FilterScannerForSort;
import org.apache.carbondata.scan.scanner.impl.NonFilterScannerForSort;

/**
 * This abstract class provides a skeletal implementation of the
 * Block iterator.
 */
public  class DataBlockForSort {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataBlockForSort.class.getName());
  /**
   * iterator which will be used to iterate over data blocks
   */
  protected CarbonIterator<DataRefNode> dataBlockIterator;

 //private List<Future<AbstractScannedResult>> scanResultfutureList = new ArrayList<Future<AbstractScannedResult>>();
  
  /**
   * execution details
   */
  protected BlockExecutionInfo blockExecutionInfo;

  /**
   * result collector which will be used to aggregate the scanned result
   */
  protected ScannedResultCollector scannerResultAggregator;
  public ScannedResultCollector getScannerResultAggregator() {
	return scannerResultAggregator;
}

public void setScannerResultAggregator(ScannedResultCollector scannerResultAggregator) {
	this.scannerResultAggregator = scannerResultAggregator;
}
  /**
   * processor which will be used to process the block processing can be
   * filter processing or non filter processing
   */
  //protected BlockletScanner blockletScanner;

  /**
   * to hold the data block
   */
  //protected BlocksChunkHolder blocksChunkHolder;
  
  private  List<BlocksChunkHolder> blocksChunkHolderList = new ArrayList<BlocksChunkHolder>();
  QueryDimension singleSortDimesion = null;
  
  public List<BlocksChunkHolder> getBlocksChunkHolderList() {
	return blocksChunkHolderList;
}

public void setBlocksChunkHolderList(List<BlocksChunkHolder> blocksChunkHolderList) {
	this.blocksChunkHolderList = blocksChunkHolderList;
}


/**
   * batch size of result
   */
  protected int batchSize;

  public AbstractScannedResult scannedResult;

  QueryStatisticsModel queryStatisticsModel;

  public DataBlockForSort(BlockExecutionInfo blockExecutionInfo,
      FileHolder fileReader, int batchSize, QueryStatisticsModel queryStatisticsModel, QueryModel queryModel, BlocksChunkHolderLimitFilter blocksChunkHolderLimitFilter)  throws QueryExecutionException{
    this.blockExecutionInfo = blockExecutionInfo;
//    dataBlockIterator = new BlockletIterator(blockExecutionInfo.getFirstDataBlock(),
//        blockExecutionInfo.getNumberOfBlockToScan());
    singleSortDimesion = queryModel.getSortDimensions().get(0);  
    DataRefNode firstDataBlock = blockExecutionInfo.getFirstDataBlock();
    
/*	blocksChunkHolderLimitFilter = new BlocksChunkHolderLimitFilter(blockExecutionInfo.getLimit(),
			blockExecutionInfo.getAllSortDimensionBlocksIndexes()[0],
			queryModel.getSortDimensions().get(0).getSortOrder());*/
		
	blocksChunkHolderLimitFilter.addBlocksChunkHolder(generateBlocksChunkHolder(blockExecutionInfo, fileReader, queryStatisticsModel,
			queryModel, firstDataBlock));
		
/*	blocksChunkHolderList.add(generateBlocksChunkHolder(blockExecutionInfo, fileReader, queryStatisticsModel,
				queryModel, firstDataBlock));
	scanResultfutureList.add(scanBlockletForSortDimension(generateBlocksChunkHolder(blockExecutionInfo, fileReader, queryStatisticsModel,
			queryModel, firstDataBlock), execService));*/

	
	firstDataBlock = firstDataBlock.getNextDataRefNode();
    while(firstDataBlock !=null){

    	blocksChunkHolderLimitFilter.addBlocksChunkHolder(generateBlocksChunkHolder(blockExecutionInfo, fileReader, queryStatisticsModel,
    			queryModel, firstDataBlock));
        //blocksChunkHolderList.add(generateBlocksChunkHolder(blockExecutionInfo, fileReader, queryStatisticsModel,
		//		queryModel, firstDataBlock));
        //scanResultfutureList.add(scanBlockletForSortDimension(tmpBlocksChunkHolder, execService));
        
        firstDataBlock = firstDataBlock.getNextDataRefNode();
    }
/*

    if (blockExecutionInfo.getFilterExecuterTree() != null) {
      blockletScanner = new FilterScannerForSort(blockExecutionInfo, queryStatisticsModel);
    } else {
      blockletScanner = new NonFilterScanner(blockExecutionInfo);
    }*/
    if (blockExecutionInfo.isRawRecordDetailQuery()) {
    	//TODO error
      //this.scannerResultAggregator =
       //   new RawBasedResultCollector(blockExecutionInfo);
    } else {
      this.scannerResultAggregator =
          new DictionaryBasedResultCollectorForSort(blockExecutionInfo);
    }
    this.batchSize = batchSize;
    this.queryStatisticsModel = queryStatisticsModel;
  }

private BlocksChunkHolder generateBlocksChunkHolder(BlockExecutionInfo blockExecutionInfo, FileHolder fileReader,
		QueryStatisticsModel queryStatisticsModel, QueryModel queryModel, DataRefNode dataBlock) throws QueryExecutionException {
	BlocksChunkHolder tmpBlocksChunkHolder;
	tmpBlocksChunkHolder = new BlocksChunkHolder(blockExecutionInfo.getTotalNumberDimensionBlock(),
	        blockExecutionInfo.getTotalNumberOfMeasureBlock());
	tmpBlocksChunkHolder.setLimit(queryModel.getLimit());
	tmpBlocksChunkHolder.setSingleSortDimesion(queryModel.getSortDimensions().get(0));
	tmpBlocksChunkHolder.setNodeSize(dataBlock.nodeSize());
	tmpBlocksChunkHolder.setFileReader(fileReader);
	tmpBlocksChunkHolder.setDataBlock(dataBlock);
	tmpBlocksChunkHolder.setMinValueForSortKey(dataBlock.getColumnsMinValue()[blockExecutionInfo.getAllSortDimensionBlocksIndexes()[0]]);
	tmpBlocksChunkHolder.setMaxValueForSortKey(dataBlock.getColumnsMaxValue()[blockExecutionInfo.getAllSortDimensionBlocksIndexes()[0]]);
	//tmpBlocksChunkHolder.setNodeNumber(dataBlock.nodeNumber());
	String path[] = dataBlock.getFilePath().split("/");
	tmpBlocksChunkHolder.setBlockletIndentifyPath(path[path.length-1] +dataBlock.nodeNumber());
	
	tmpBlocksChunkHolder.setBlockExecutionInfo(blockExecutionInfo);
	if (blockExecutionInfo.getFilterExecuterTree() != null) {
		FilterScannerForSort filter = new FilterScannerForSort(blockExecutionInfo, queryStatisticsModel);
		if(filter.applyFilter(tmpBlocksChunkHolder)){
			return null;
		}
		tmpBlocksChunkHolder.setBlockletScanner(filter);
	} else {
		tmpBlocksChunkHolder.setBlockletScanner(new NonFilterScannerForSort(blockExecutionInfo));
	}
	return tmpBlocksChunkHolder;
}

 
  /* public boolean hasNext() {
    if (scannedResult != null && scannedResult.hasNext()) {
      return true;
    } else {
      return dataBlockIterator.hasNext();
    }
  }

  protected boolean updateScanner() {
    //try {
      if (scannedResult != null && scannedResult.hasNext()) {
        return true;
      } else {
        //scannedResult = getNextScannedResult();
        while (scannedResult != null) {
          if (scannedResult.hasNext()) {
            return true;
          }
         // scannedResult = getNextScannedResult();
        }
        return false;
      }
//    } catch (QueryExecutionException ex) {
//      throw new RuntimeException(ex);
//    }
  }*/



  public AbstractScannedResult getNextScannedResult(BlocksChunkHolder blocksChunkHolder) throws QueryExecutionException {

	  //only handle single dimension sort
		AbstractScannedResult scanResult = blocksChunkHolder.scanBlockletForSort(singleSortDimesion.getSortOrder());
		scanResult.setScannerResultAggregator(scannerResultAggregator);
		//if (scanResult.numberOfOutputRows() > 0) {
			//scanResult.initCurrentDictionaryKeyForSortDimention(singleSortDimesion.getSortOrder());
		//}

		return scanResult;

  }

  
  /**
   * TODO It scans the block and returns the result with @batchSize
   *
   * @return Result of @batchSize
   */
/*  public AbstractScannedResult nextScannedResult() {
    
    if (updateScanner()) {
      return scannedResult;
    } else {
      return null;
    }
  }*/
  

}
