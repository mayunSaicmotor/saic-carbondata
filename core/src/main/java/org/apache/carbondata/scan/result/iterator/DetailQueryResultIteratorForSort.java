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
package org.apache.carbondata.scan.result.iterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.model.QueryDimension;
import org.apache.carbondata.scan.model.QueryModel;
import org.apache.carbondata.scan.model.SortOrderType;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.scan.processor.BlocksChunkHolderLimitFilter;
import org.apache.carbondata.scan.processor.DataBlockForSort;
import org.apache.carbondata.scan.result.AbstractScannedResult;
import org.apache.carbondata.scan.result.BatchResult;
import org.apache.carbondata.scan.result.ScanResultComparator;

/**
 * In case of detail query we cannot keep all the records in memory so for
 * executing that query are returning a iterator over block and every time next
 * call will come it will execute the block and return the result
 */
public class DetailQueryResultIteratorForSort extends AbstractDetailQueryResultIterator {

	private final Object lock = new Object();
	Integer executeCnt =0;
	private Future<BatchResult> future;
	//private Integer limitKey = null;
	private SortOrderType sortType = SortOrderType.ASC;
	AbstractScannedResult currentScannedResult = null;
	AbstractScannedResult nextScannedResult = null;
	
	//boolean hasNextFlg =true;
	
	protected TreeSet<AbstractScannedResult> scannedResultSet;

	protected List<Future<AbstractScannedResult>> scanResultfutureList = new ArrayList<Future<AbstractScannedResult>>();
	//protected List<Future<AbstractScannedResult>> nextScanResultfutureList = new ArrayList<Future<AbstractScannedResult>>();

	public DetailQueryResultIteratorForSort(List<BlockExecutionInfo> infos, QueryModel queryModel,
			ExecutorService execService)  throws QueryExecutionException{
		super(infos, queryModel, execService);
		
		//TODO only consider the single dimension sort
		QueryDimension singleSortDimesion = queryModel.getSortDimensions().get(0);
		sortType = singleSortDimesion.getSortOrder();
		scannedResultSet = new TreeSet<AbstractScannedResult>(new ScanResultComparator(sortType));
		generateBlocketForSort(queryModel);

		
	}

	private static final LogService LOGGER = LogServiceFactory
			.getLogService(DetailQueryResultIteratorForSort.class.getName());


/*	public DataBlockIteratorImpl getDataBlocketsForSort() {

		int batchSize = 0;
		String batchSizeString = CarbonProperties.getInstance()
				.getProperty(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE);
		if (null != batchSizeString) {
			try {
				batchSize = Integer.parseInt(batchSizeString);
			} catch (NumberFormatException ne) {
				LOGGER.error("Invalid inmemory records size. Using default value");
				batchSize = CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT;
			}
		} else {
			batchSize = CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT;
		}

		if (blockExecutionInfos.size() > 0) {
			BlockExecutionInfo executionInfo = blockExecutionInfos.get(0);
			blockExecutionInfos.remove(executionInfo);
			queryStatisticsModel.setRecorder(recorder);
			return new DataBlockIteratorImpl(executionInfo, fileReader, batchSize, queryStatisticsModel);
		}
		return null;
	}*/

	@Override
	public BatchResult next() {
		BatchResult result = null;
		//klong startTime = System.currentTimeMillis();
		try {

/*			// get all sorted scannedResults
			
			if (scannedResultSet.size() == 0) {
				//long start = System.currentTimeMillis();
				for (Future<AbstractScannedResult> futureResult : futureList) {
					AbstractScannedResult detailedScannedResult = futureResult.get();
					LOGGER.info("detailedScannedResult.getCurrentSortDimentionKey()" + detailedScannedResult.getCurrentSortDimentionKey());
					if(detailedScannedResult != null){
						scannedResultSet.add(detailedScannedResult);
					}
					
				}

				//long end = System.currentTimeMillis();
				LOGGER.info("get executeScan: "+(end -start));
			}*/


			if (future == null) {
				//this.printTreeSet();
				future = execute();
			}
			result = future.get();
			nextBatch = false;
			if (hasNext()) {
				nextBatch = true;
				future = execute();
			} else {
				fileReader.finish();
			}
			//totalScanTime += System.currentTimeMillis() - startTime;
		} catch (Exception ex) {
			fileReader.finish();
			throw new RuntimeException(ex);
		}
		
/*		synchronized(this.getClass()){
			LOGGER.info("start to print");
			for(Object[] arr : result.getRows()){
				LOGGER.info(arr[2].toString());
			}
		}*/

		return result;
	}

	@Override
	public boolean hasNext() {

		//return true;
		//return hasNextFlg;
		
		if(nextBatch){
			return true;
		}
		boolean nextFlg = false;
		if(limitFlg){
			
			nextFlg = limit > 0 
					&& (scannedResultSet.size() > 0 
							|| (currentScannedResult != null 
								&& currentScannedResult.hasNextForSort()));
		}else {
			
			nextFlg = scannedResultSet.size() > 0
					|| (this.currentScannedResult != null 
						&& this.currentScannedResult.hasNextForSort());
		}
		//boolean nextFlg = (limit > 0  && (scannedResultSet.size() > 0 || this.currentScannedResult.hasNextForSort() ) ) || nextBatch;
		//LOGGER.info("hasNext: "+nextFlg);
		//TODO
//		if(	!nextFlg){
//			
//			LOGGER.info("currentScannedResult getCurrentSortDimentionKey: "+currentScannedResult.getCurrentSortDimentionKey());
//		}
		return nextFlg;
	}

	/**
	 * It scans the block and returns the result with @batchSize
	 *
	 * @return Result of @batchSize
	 */
	public Future<BatchResult> execute() {

		return execService.submit(new Callable<BatchResult>() {
			@Override
			public BatchResult call() throws QueryExecutionException {

				BatchResult batchResult = new BatchResult();

				List<Object[]> collectedResult = null;
				//long start =System.currentTimeMillis();
				//int tmpCount = 0;
				//boolean tmpflg =updateScanner();
				//LOGGER.info("updateScanner: "+tmpflg);
				if (updateScanner()) {
					
					collectedResult = currentScannedResult.collectSortedData(batchSize, nextScannedResult !=null?nextScannedResult.getCurrentSortDimentionKey():null);

					decreaseLimit(collectedResult.size());	
					
					//long end =System.currentTimeMillis();
					//LOGGER.info("collectSortedData"+(++tmpCount) +": " +(end -start));
					
					//tmpflg =updateScanner();
					//LOGGER.info("updateScanner: "+tmpflg);
					while (collectedResult.size() < batchSize && limit>0 && updateScanner()) {
						
						  //start =System.currentTimeMillis();
						//List<Object[]> data = currentScannedResult.collectSortedData( batchSize - collectedResult.size(), nextScannedResult !=null?nextScannedResult.getCurrentSortDimentionKey():null);
						//currentScannedResult.loadOtherColunmsData();
						int leftSize = batchSize - collectedResult.size();
						if(limitFlg &&(limit < leftSize)){
							leftSize = limit;
						}
						List<Object[]> data = currentScannedResult.collectSortedData( leftSize, nextScannedResult !=null?nextScannedResult.getCurrentSortDimentionKey():null);
						collectedResult.addAll(data);

						decreaseLimit(data.size());
						//printCount(collectedResult);
						
						///tmpflg =updateScanner();
						//LOGGER.info("updateScanner: "+tmpflg);
						 //end =System.currentTimeMillis();
						//LOGGER.info("collectSortedData "+(++tmpCount) +": " +(end -start));
					}
					

				} else {
					collectedResult = new ArrayList<>();
					
				}
				batchResult.setRows(collectedResult);
				//if(collectedResult!=null){
					//LOGGER.info("collectedResult start: " + collectedResult.get(0)[0]);
					//LOGGER.info("collectedResult end: " + collectedResult.get(collectedResult.size()-1)[0]);
				//}
				//LOGGER.info("collectedResult.size(): "+collectedResult.size());
				return batchResult;

			}

			public void printCount(List<Object[]> collectedResult) {
				synchronized (executeCnt){
					LOGGER.info("executeCnt:"+  ++executeCnt);
					//limit = limit -collectedResult.size();
					LOGGER.info("limit left:"+  limit);
					LOGGER.info("collectedResult.size:"+  collectedResult.size());
				}
			}
		});
	}

	protected boolean updateScanner() {
		
		//printTreeSet();
//   		if(currentScannedResult != null && "name999887".equals(currentScannedResult.getCurrentSortDimentionKey())){
//   	   		if( "name999887".equals(currentScannedResult.getCurrentSortDimentionKey())){
//   	   			LOGGER.info("collectedResult.size:");
//   	   		}
//
//		}

		if (currentScannedResult != null) {
			if(currentScannedResult.hasNextForSort()){
				
				//scannedResultSet.remove(currentScannedResult);				
				//printTreeSet();
				if(! currentScannedResult.isCurrentSortDimentionKeyChgFlg()){
				
					return true;
				}
				scannedResultSet.add(currentScannedResult);				
				//printTreeSet();
			}else{
				
				scannedResultSet.remove(currentScannedResult);
/*				if(scannedResultSet.size()<=0){
					//hasNextFlg = false;
					return false;
				}*/
			}
		}
	
		if(scannedResultSet.size()>0){
			currentScannedResult = scannedResultSet.first();
			//printTreeSet();
//	   		if(currentScannedResult != null && "name999887".equals(currentScannedResult.getCurrentSortDimentionKey())){
//	   	   		if( "name999887".equals(currentScannedResult.getCurrentSortDimentionKey())){
//	   	   			LOGGER.info("collectedResult.size:");
//	   	   		}
//
//			}
	   		
			scannedResultSet.remove(currentScannedResult);
			if(scannedResultSet.size()>0){
				nextScannedResult = scannedResultSet.first();
			} else {
				nextScannedResult = null;
			}
		}else{
			//hasNextFlg = false;
			return false;
		}

		
		
		//printTreeSet();
		
/*		Iterator<AbstractScannedResult> it =   scannedResultSet.iterator();
		if(it.hasNext()){
			currentScannedResult  = it.next();
		}
		
		//second
		if(it.hasNext()){
			nextScannedResult  = it.next();
		}*/
		

		return true;
	}

	private void printTreeSet() {
		LOGGER.info("start print");
		for(AbstractScannedResult detailedScannedResult : this.scannedResultSet){
			
			LOGGER.info("CurrentSortDimentionKey in scannedResultSet: " + detailedScannedResult.getCurrentSortDimentionKey());	
		}
		LOGGER.info("end print");
	}



	private void generateBlocketForSort(QueryModel queryModel)  throws QueryExecutionException{

		//LOGGER.info("blockExecutionInfos: "+ blockExecutionInfos.get(0).getNumberOfBlockToScan());
		
//		//according to the limit value to filter some blocklets
//		if("0".equals(queryModel.getTableBlockInfos().get(0).getSegmentId())){
//			LOGGER.info("getFilePath: " + queryModel.getTableBlockInfos().get(0).getFilePath());	
//		}
		BlocksChunkHolderLimitFilter blocksChunkHolderLimitFilter = new BlocksChunkHolderLimitFilter(queryModel.getLimit(),
				blockExecutionInfos.get(0).getAllSortDimensionBlocksIndexes()[0],
				queryModel.getSortDimensions().get(0).getSortOrder());
		List<DataBlockForSort> dataBlocksList = new ArrayList<DataBlockForSort>();
		for (BlockExecutionInfo executionInfo : blockExecutionInfos) {

			// BlockExecutionInfo executionInfo = blockExecutionInfos.get(0);
			// blockExecutionInfos.remove(executionInfo);
			queryStatisticsModel.setRecorder(recorder);
			dataBlocksList.add(new DataBlockForSort(executionInfo, fileReader, batchSize, queryStatisticsModel, queryModel, blocksChunkHolderLimitFilter));
		}
		blockExecutionInfos = null;
		
		//long start = System.currentTimeMillis();
		//LOGGER.info("blocksChunkHolderLimitFilter size: "+blocksChunkHolderLimitFilter.getRequiredToScanBlocksChunkHolderSet().size());
		generateAllScannedResultSet(blocksChunkHolderLimitFilter);
		//long end = System.currentTimeMillis();
		//LOGGER.info("generateAllScannedResultSet: "+(end -start));
	}

	
	  public AbstractScannedResult getNextScannedResult(BlocksChunkHolder blocksChunkHolder) throws QueryExecutionException {

		  //only handle single dimension sort
			AbstractScannedResult scanResult = blocksChunkHolder.scanBlockletForSort(sortType);
			//scanResult.setScannerResultAggregator(scannerResultAggregator);
			//if (scanResult.numberOfOutputRows() > 0) {
				//scanResult.initCurrentDictionaryKeyForSortDimention(singleSortDimesion.getSortOrder());
			//}

			return scanResult;

	  }
	  
	private void generateAllScannedResultSet(BlocksChunkHolderLimitFilter blocksChunkHolderLimitFilter) {

		TreeSet<BlocksChunkHolder> requiredToScanBlocksChunkHolderSet = blocksChunkHolderLimitFilter.getRequiredToScanBlocksChunkHolderSet();

		try {
			
			Iterator<BlocksChunkHolder> it = requiredToScanBlocksChunkHolderSet.iterator();
			
			//byte[] current = blocksChunkHolderLimitFilter.getMinValueForSortKey();
			while(it.hasNext()){
				BlocksChunkHolder blocksChunkHolder  = it.next();
				
				//each time scan the equal min blocklet
				//if(Arrays.equals(current, blocksChunkHolder.getMinValueForSortKey())){
					
/*					int dict=0;
				    for (int i = 0; i < blocksChunkHolder.getMinValueForSortKey().length; i++) {
				        dict <<= 8;
				        dict ^= blocksChunkHolder.getMinValueForSortKey()[i] & 0xFF;
				      }*/
				    
					//LOGGER.info("blocksChunkHolder min value: " + dict);
					scanResultfutureList.add(scanBlockletForSortDimension(blocksChunkHolder));

				//}

			}
			
			
			//long start = System.currentTimeMillis();
			for (int i = 0; i < scanResultfutureList.size(); i++) {
				Future<AbstractScannedResult> futureResult = scanResultfutureList.get(i);

				AbstractScannedResult detailedScannedResult = futureResult.get();

				// LOGGER.info("detailedScannedResult.getCurrentSortDimentionKey()"
				// + detailedScannedResult.getCurrentSortDimentionKey());
				if (detailedScannedResult != null && detailedScannedResult.numberOfOutputRows() > 0) {
					
					scannedResultSet.add(detailedScannedResult);

				}else{
					LOGGER.info("null result set");
				}

			}

			/*	long end = System.currentTimeMillis();
			LOGGER.info("get executeScan: " + (end - start));
			
		for (final BlocksChunkHolder blocksChunkHolder : requiredToScanBlocksChunkHolderSet) {
				
				int dict=0;
			    for (int i = 0; i < blocksChunkHolder.getMinValueForSortKey().length; i++) {
			        dict <<= 8;
			        dict ^= blocksChunkHolder.getMinValueForSortKey()[i] & 0xFF;
			      }
			    
				LOGGER.info("blocksChunkHolder min value: " + dict);
				scanResultfutureList.add(scanBlockletForSortDimension(blocksChunkHolder)

				);

			}*/

		} catch (Exception ex) {
			fileReader.finish();
			throw new RuntimeException(ex);
		}
	}

	private Future<AbstractScannedResult> scanBlockletForSortDimension(final BlocksChunkHolder blocksChunkHolder) {
		return execService.submit(new Callable<AbstractScannedResult>() {
			@Override
			public AbstractScannedResult call() throws QueryExecutionException {
				// TODO maybe the lock can be removed
				 //synchronized (lock) {
				 //synchronized (fileReader) {
					//long start = System.currentTimeMillis();
					AbstractScannedResult scanResult = getNextScannedResult(blocksChunkHolder);
					//long end = System.currentTimeMillis();
					//LOGGER.info("get NextScannedResult: "+(end -start));
					//scanResult.setScannerResultAggregator(dataBlockForSort.getScannerResultAggregator());
					if(scanResult.numberOfOutputRows()>0){
						//scanResult.initCurrentDictionaryKeyForSortDimention(sortType);
						return scanResult;
					}
					//scanResult.initCurrentDictionaryKeyForSortDimention(sortType);
					//scanResult.setSortType(sortType);
					return null;
				 //}

			}
		})

		;
	}


}
