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
package org.apache.carbondata.scan.scanner;

import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.model.SortOrderType;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.scan.result.AbstractScannedResult;

/**
 * Blocklet scanner class to process the block
 */
public abstract class AbstractBlockletScanner implements BlockletScanner {

  /**
   * scanner result
   */
  protected AbstractScannedResult scannedResult;

  public AbstractScannedResult  getScannedResultAfterProcessFilter(){
	  
	  return scannedResult;
	  
  }
  /**
   * block execution info
   */
  protected BlockExecutionInfo blockExecutionInfo;

  public AbstractBlockletScanner(BlockExecutionInfo tableBlockExecutionInfos) {
	  
	 // System.out.println("tableBlockExecutionInfos.getAllSortDimensionBlocksIndexes()[0]: "+tableBlockExecutionInfos.getAllSortDimensionBlocksIndexes()[0]);
    this.blockExecutionInfo = tableBlockExecutionInfos;
  }

  @Override public AbstractScannedResult scanBlocklet(BlocksChunkHolder blocksChunkHolder)
      throws QueryExecutionException {
    fillKeyValue(blocksChunkHolder);
    return scannedResult;
  }
  
  @Override public AbstractScannedResult scanBlockletForSort(BlocksChunkHolder blocksChunkHolder, SortOrderType orderType)
	      throws QueryExecutionException {
	  fillKeyValueForSort(blocksChunkHolder, orderType);
	    return scannedResult;
	  }

  protected void fillKeyValue(BlocksChunkHolder blocksChunkHolder) {
    scannedResult.reset();
    
    //long start = System.currentTimeMillis();
    
    scannedResult.setMeasureChunks(blocksChunkHolder.getDataBlock()
        .getMeasureChunks(blocksChunkHolder.getFileReader(),
            blockExecutionInfo.getAllSelectedMeasureBlocksIndexes(), blockExecutionInfo.getLimit()));

    scannedResult.setNumberOfRows(blocksChunkHolder.getDataBlock().nodeSize());

    //long end = System.currentTimeMillis();
   //	System.out.println("fillKeyValue1: "+(end -start));
   	//start = System.currentTimeMillis();
   	
    scannedResult.setDimensionChunks(blocksChunkHolder.getDataBlock()
        .getDimensionChunks(blocksChunkHolder.getFileReader(),
            blockExecutionInfo.getAllSelectedDimensionBlocksIndexes()));
    
     //end = System.currentTimeMillis();
	//System.out.println("fillKeyValue2: "+(end -start) + " blocksChunkHolder.getFileReader(): " + blocksChunkHolder.getFileReader().toString());
	
    //TODO
    scannedResult.setAllSortDimensionBlocksIndexes();
  }
  
  // TODO
  protected void fillKeyValueForSort(BlocksChunkHolder blocksChunkHolder, SortOrderType orderType) {
	    scannedResult.reset();
	    
	    //long start = System.currentTimeMillis();
	    
/*	    scannedResult.setMeasureChunks(blocksChunkHolder.getDataBlock()
	        .getMeasureChunks(blocksChunkHolder.getFileReader(),
	            blockExecutionInfo.getAllSelectedMeasureBlocksIndexes()));

	    scannedResult.setNumberOfRows(blocksChunkHolder.getDataBlock().nodeSize());*/

	    //long end = System.currentTimeMillis();
	   //	System.out.println("fillKeyValue1: "+(end -start));
	   	//start = System.currentTimeMillis();
		DimensionColumnDataChunk[] dimensionColumnDataChunks = blocksChunkHolder.getDataBlock()
				.getDimensionChunksForSort(blocksChunkHolder.getFileReader(),
						blockExecutionInfo.getAllSortDimensionBlocksIndexes(), blockExecutionInfo.getLimit(), SortOrderType.DSC.equals(orderType));
	    scannedResult.setDimensionChunks(dimensionColumnDataChunks);
	    scannedResult.setNumberOfRows(blocksChunkHolder.getDataBlock().nodeSize());
	    
	    if(!scannedResult.isFilterQueryFlg()){
	    	int[] invertedIndexesReverse = dimensionColumnDataChunks[blockExecutionInfo.getAllSortDimensionBlocksIndexes()[0]].getAttributes().getInvertedIndexesReverse();
	    	if(invertedIndexesReverse!=null){
	    		scannedResult.setMaxLogicalRowIdByLimit(invertedIndexesReverse.length-1);
	    	}else{
	    		
	    		scannedResult.setMaxLogicalRowIdByLimit(scannedResult.getDimensionChunks()[scannedResult.getAllSortDimensionBlocksIndexes()[0]].getTotalRowNumber());
	    	}
	    	
	    }

	    
	     //end = System.currentTimeMillis();
		//System.out.println("fillKeyValue2: "+(end -start) + " blocksChunkHolder.getFileReader(): " + blocksChunkHolder.getFileReader().toString());
		
	    //TODO
	    scannedResult.setLoadDataDelay(true);
	    scannedResult.setBlocksChunkHolder(blocksChunkHolder);
	    scannedResult.setBlockletIndentifyPath(blocksChunkHolder.getBlockletIndentifyPath());//(blocksChunkHolder.getNodeNumber());
	    scannedResult.setAllSortDimensionBlocksIndexes();
	    scannedResult.initCurrentKeyForSortDimention(orderType);
	  }
}
