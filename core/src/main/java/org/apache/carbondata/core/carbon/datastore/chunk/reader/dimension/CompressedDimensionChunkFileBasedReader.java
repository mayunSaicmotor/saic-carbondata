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
package org.apache.carbondata.core.carbon.datastore.chunk.reader.dimension;

import java.util.List;

import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.ColumnGroupDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.DataChunk;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.columnar.UnBlockIndexer;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Compressed dimension chunk reader class
 */
public class CompressedDimensionChunkFileBasedReader extends AbstractChunkReader {

  /**
   * Constructor to get minimum parameter to create instance of this class
   *
   * @param dimensionColumnChunk dimension chunk metadata
   * @param eachColumnValueSize  size of the each column value
   * @param filePath             file from which data will be read
   */
  public CompressedDimensionChunkFileBasedReader(List<DataChunk> dimensionColumnChunk,
      int[] eachColumnValueSize, String filePath) {
    super(dimensionColumnChunk, eachColumnValueSize, filePath);
  }

  /**
   * Below method will be used to read the chunk based on block indexes
   *
   * @param fileReader   file reader to read the blocks from file
   * @param blockIndexes blocks to be read
   * @return dimension column chunks
   */
  @Override public DimensionColumnDataChunk[] readDimensionChunks(FileHolder fileReader,
      int... blockIndexes) {
    // read the column chunk based on block index and add
    DimensionColumnDataChunk[] dataChunks =
        new DimensionColumnDataChunk[dimensionColumnChunk.size()];
    for (int i = 0; i < blockIndexes.length; i++) {
      dataChunks[blockIndexes[i]] = readDimensionChunk(fileReader, blockIndexes[i]);
    }
    return dataChunks;
  }

  //TODO
  /**
   * Below method will be used to read the chunk based on block indexes
   *
   * @param fileReader   file reader to read the blocks from file
   * @param blockIndexes blocks to be read
   * @return dimension column chunks
   */
  @Override public DimensionColumnDataChunk[] readDimensionChunksForSort(int[] sortDimentionBlockIndexes, FileHolder fileReader,int pageSize, int limit, boolean descSortFlg) {
    // read the column chunk based on block index and add
    DimensionColumnDataChunk[] dataChunks =
        new DimensionColumnDataChunk[dimensionColumnChunk.size()];
    for (int i = 0; i < sortDimentionBlockIndexes.length; i++) {
      dataChunks[sortDimentionBlockIndexes[i]] = readDimensionChunkForSort(fileReader, sortDimentionBlockIndexes[i], pageSize, limit, descSortFlg);
    }
    return dataChunks;
  }
  
  /**
   * Below method will be used to read the chunk based on block index
   *
   * @param fileReader file reader to read the blocks from file
   * @param blockIndex block to be read
   * @return dimension column chunk
   */
  @Override public DimensionColumnDataChunk readDimensionChunk(FileHolder fileReader,
      int blockIndex) {
    byte[] dataPage = null;
    int[] invertedIndexes = null;
    int[] invertedIndexesReverse = null;
    int[] rlePage = null;

    // first read the data and uncompressed it
    dataPage = COMPRESSOR.unCompress(fileReader
        .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getDataPageOffset(),
            dimensionColumnChunk.get(blockIndex).getDataPageLength()));
    // if row id block is present then read the row id chunk and uncompress it
    if (CarbonUtil.hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(),
        Encoding.INVERTED_INDEX)) {
      invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(dimensionColumnChunk.get(blockIndex).getRowIdPageLength(),
              fileReader.readByteArray(filePath,
                  dimensionColumnChunk.get(blockIndex).getRowIdPageOffset(),
                  dimensionColumnChunk.get(blockIndex).getRowIdPageLength()), numberComressor);
      // get the reverse index
      invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes);
    }
    // if rle is applied then read the rle block chunk and then uncompress
    //then actual data based on rle block
    if (CarbonUtil
        .hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(), Encoding.RLE)) {
      // read and uncompress the rle block
      rlePage = numberComressor.unCompress(fileReader
          .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getRlePageOffset(),
              dimensionColumnChunk.get(blockIndex).getRlePageLength()));
      // uncompress the data with rle indexes
      dataPage = UnBlockIndexer.uncompressData(dataPage, rlePage, eachColumnValueSize[blockIndex]);
      rlePage = null;
    }
    // fill chunk attributes
    DimensionChunkAttributes chunkAttributes = new DimensionChunkAttributes();
    chunkAttributes.setEachRowSize(eachColumnValueSize[blockIndex]);
    chunkAttributes.setInvertedIndexes(invertedIndexes);
    chunkAttributes.setInvertedIndexesReverse(invertedIndexesReverse);
    DimensionColumnDataChunk columnDataChunk = null;

    if (dimensionColumnChunk.get(blockIndex).isRowMajor()) {
      // to store fixed length column chunk values
      columnDataChunk = new ColumnGroupDimensionDataChunk(dataPage, chunkAttributes);
    }
    // if no dictionary column then first create a no dictionary column chunk
    // and set to data chunk instance
    else if (!CarbonUtil
        .hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(), Encoding.DICTIONARY)) {
      columnDataChunk = new VariableLengthDimensionDataChunk(getNoDictionaryDataChunk(dataPage), chunkAttributes);
      chunkAttributes.setNoDictionary(true);
    } else {
      // to store fixed length column chunk values
      columnDataChunk = new FixedLengthDimensionDataChunk(dataPage, chunkAttributes);
    }
    return columnDataChunk;
  }
  
  /**
   * Below method will be used to read the chunk based on block index
   *
   * @param fileReader file reader to read the blocks from file
   * @param blockIndex block to be read
   * @return dimension column chunk
   */
  @Override public DimensionColumnDataChunk readDimensionChunkForFilter(FileHolder fileReader,
      int blockIndex, int limit, int maxLogicalRowId, boolean descSortFlg) {
    byte[] dataPage = null;
    int[] invertedIndexes = null;
    int[] invertedIndexesReverse = null;
    int[] rlePage = null;

    // first read the data and uncompressed it
    dataPage = COMPRESSOR.unCompress(fileReader
        .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getDataPageOffset(),
            dimensionColumnChunk.get(blockIndex).getDataPageLength()));
    // if row id block is present then read the row id chunk and uncompress it
    if (CarbonUtil.hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(),
        Encoding.INVERTED_INDEX)) {
      invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(dimensionColumnChunk.get(blockIndex).getRowIdPageLength(),
              fileReader.readByteArray(filePath,
                  dimensionColumnChunk.get(blockIndex).getRowIdPageOffset(),
                  dimensionColumnChunk.get(blockIndex).getRowIdPageLength()), numberComressor);
      // get the reverse index
      if(descSortFlg){
    	  invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes);
      }else{
    	  invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes, maxLogicalRowId);
      }
    }
    // if rle is applied then read the rle block chunk and then uncompress
    //then actual data based on rle block
    if (CarbonUtil
        .hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(), Encoding.RLE)) {
      // read and uncompress the rle block
      rlePage = numberComressor.unCompress(fileReader
          .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getRlePageOffset(),
              dimensionColumnChunk.get(blockIndex).getRlePageLength()));
      // uncompress the data with rle indexes
      dataPage = UnBlockIndexer.uncompressData(dataPage, rlePage, eachColumnValueSize[blockIndex]);
      rlePage = null;
    }
    // fill chunk attributes
    DimensionChunkAttributes chunkAttributes = new DimensionChunkAttributes();
    chunkAttributes.setEachRowSize(eachColumnValueSize[blockIndex]);
    chunkAttributes.setInvertedIndexes(invertedIndexes);
    chunkAttributes.setInvertedIndexesReverse(invertedIndexesReverse);
    DimensionColumnDataChunk columnDataChunk = null;

    if (dimensionColumnChunk.get(blockIndex).isRowMajor()) {
      // to store fixed length column chunk values
      columnDataChunk = new ColumnGroupDimensionDataChunk(dataPage, chunkAttributes);
    }
    // if no dictionary column then first create a no dictionary column chunk
    // and set to data chunk instance
    else if (!CarbonUtil
        .hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(), Encoding.DICTIONARY)) {
/*    	synchronized(this.getClass()){
    		if (invertedIndexesReverse != null) {
    			System.out.println("limit : " + limit);
    			System.out.println("invertedIndexesReverse length: " + invertedIndexesReverse.length);
    		}
    		
    		long start = System.currentTimeMillis();
	    	for(int i = 0; i <100; i++){
	    		columnDataChunk = new VariableLengthDimensionDataChunk(getNoDictionaryDataChunk(dataPage), chunkAttributes);
	
	    	}
	    	System.out.println("compare 1: " +(System.currentTimeMillis() -start));
	    	start = System.currentTimeMillis();
	    	for(int i = 0; i <100; i++){
				if (invertedIndexesReverse != null) {
					maxLogicalRowId = UnBlockIndexer.getMaxInt(invertedIndexesReverse) + 1;
				}
				columnDataChunk = new VariableLengthDimensionDataChunk(getNoDictionaryDataChunk(dataPage, maxLogicalRowId),
						chunkAttributes);
	       	}
	    	System.out.println("compare 2: " +(System.currentTimeMillis() -start));
    	}*/
    	
/*		if (invertedIndexesReverse != null) {
			if(invertedIndexesReverse.length < CarbonCommonConstants.MAX_SIZE_FOR_GET_MAXINT_FROM_ARRAY){
				System.out.println("invertedIndexesReverse.length: "+invertedIndexesReverse.length);
				maxLogicalRowId = UnBlockIndexer.getMaxInt(invertedIndexesReverse) + 1;
				columnDataChunk = new VariableLengthDimensionDataChunk(getNoDictionaryDataChunk(dataPage, maxLogicalRowId),
					chunkAttributes);		
			}else{
				columnDataChunk = new VariableLengthDimensionDataChunk(getNoDictionaryDataChunk(dataPage),
					chunkAttributes);
			}
		}else{
			columnDataChunk = new VariableLengthDimensionDataChunk(getNoDictionaryDataChunk(dataPage, maxLogicalRowId),
					chunkAttributes);
		}*/

		columnDataChunk = new VariableLengthDimensionDataChunk(getNoDictionaryDataChunk(dataPage),
				chunkAttributes);
      chunkAttributes.setNoDictionary(true);
    } else {
      // to store fixed length column chunk values
      columnDataChunk = new FixedLengthDimensionDataChunk(dataPage, chunkAttributes);
    }
    return columnDataChunk;
  }
  /**
   * Below method will be used to read the chunk based on block index
   *
   * @param fileReader file reader to read the blocks from file
   * @param blockIndex block to be read
   * @return dimension column chunk
   */
  @Override public DimensionColumnDataChunk readDimensionChunkForSort(FileHolder fileReader,
      int blockIndex, int pageSize, int limit, boolean descSortFlg) {
    byte[] dataPage = null;
    int[] invertedIndexes = null;
    int[] invertedIndexesReverse = null;
    int[] rlePage = null;
    
    // reset limit
    limit = limit>0 && limit<pageSize?limit:pageSize;
    		
    // first read the data and uncompressed it
    //long start = System.currentTimeMillis();
//    if(descSortFlg){
//    	dataPage = COMPRESSOR.unCompress(fileReader
//    	        .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getDataPageOffset(),
//    	            dimensionColumnChunk.get(blockIndex).getDataPageLength()));
//    }else{
        dataPage = COMPRESSOR.unCompress(fileReader
                .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getDataPageOffset(),
                    dimensionColumnChunk.get(blockIndex).getDataPageLength()), limit*eachColumnValueSize[blockIndex], descSortFlg);
    //}

    //long end = System.currentTimeMillis();
    
 /*   if((end -start) >100){
    	System.out.println("read dataPage: "+(end -start) + "thread: " + Thread.currentThread().getId());
    }*/
	
	
	//start = System.currentTimeMillis();
    // if row id block is present then read the row id chunk and uncompress it
    if (CarbonUtil.hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(),
        Encoding.INVERTED_INDEX)) {
  	  // start = System.currentTimeMillis();  
      invertedIndexes = CarbonUtil
          .getUnCompressColumnIndex(dimensionColumnChunk.get(blockIndex).getRowIdPageLength(),
              fileReader.readByteArray(filePath,
                  dimensionColumnChunk.get(blockIndex).getRowIdPageOffset(),
                  dimensionColumnChunk.get(blockIndex).getRowIdPageLength()), numberComressor, limit, descSortFlg);
	   //end = System.currentTimeMillis();
	  //System.out.println("time: " + (end -start));
      
    
      
      // maxLogicalRowId
      //TODO
      if(invertedIndexes.length  < CarbonCommonConstants.MAX_SIZE_FOR_GET_MAXINT_FROM_ARRAY && !descSortFlg){
    	  
          int maxLogicalRowId = UnBlockIndexer.getMaxInt(invertedIndexes);
          invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes, maxLogicalRowId+1);
    	  
      }else{
    	  invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes, pageSize);
      }
//      if(UnBlockIndexer.getMaxInt(invertedIndexes) >= invertedIndexesReverse.length){
//    	  System.out.println("fadskfjklajgjklgakljgal");
//      }
     // invertedIndexesReverse = getInvertedReverseIndex(invertedIndexes, pageSize);

    }
    
/*    end = System.currentTimeMillis();
    if((end -start) >100){
    	System.out.println("read inverted Indexes: "+(end -start) + "thread: " + Thread.currentThread().getId());
    }*/
	
	//start = System.currentTimeMillis();
	
    // if rle is applied then read the rle block chunk and then uncompress
    //then actual data based on rle block
    if (CarbonUtil
        .hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(), Encoding.RLE)) {
      // read and uncompress the rle block
//      rlePage = numberComressor.unCompress(fileReader
//          .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getRlePageOffset(),
//              dimensionColumnChunk.get(blockIndex).getRlePageLength()));
      
      rlePage = numberComressor.unCompress(fileReader
              .readByteArray(filePath, dimensionColumnChunk.get(blockIndex).getRlePageOffset(),
                  dimensionColumnChunk.get(blockIndex).getRlePageLength()), limit * 2, descSortFlg);
      // uncompress the data with rle indexes
      //dataPage = UnBlockIndexer.uncompressData(dataPage, rlePage, eachColumnValueSize[blockIndex]);
      dataPage = UnBlockIndexer.uncompressDataForSort(dataPage, rlePage, eachColumnValueSize[blockIndex], limit, descSortFlg);
      //rlePage = null;
    }
    
/*    end = System.currentTimeMillis();
    if((end -start) >100){
    	System.out.println("read rle data page: "+(end -start) + "thread: " + Thread.currentThread().getId());
    }
	start = System.currentTimeMillis();*/
	
    // fill chunk attributes
    DimensionChunkAttributes chunkAttributes = new DimensionChunkAttributes();
    chunkAttributes.setEachRowSize(eachColumnValueSize[blockIndex]);
    chunkAttributes.setInvertedIndexes(invertedIndexes);
    chunkAttributes.setInvertedIndexesReverse(invertedIndexesReverse);
    DimensionColumnDataChunk columnDataChunk = null;

    if (dimensionColumnChunk.get(blockIndex).isRowMajor()) {
      // to store fixed length column chunk values
      columnDataChunk = new ColumnGroupDimensionDataChunk(dataPage, chunkAttributes);
    }
    // if no dictionary column then first create a no dictionary column chunk
    // and set to data chunk instance
    else if (!CarbonUtil
        .hasEncoding(dimensionColumnChunk.get(blockIndex).getEncodingList(), Encoding.DICTIONARY)) {
      columnDataChunk =
          new VariableLengthDimensionDataChunk(getNoDictionaryDataChunk(dataPage), chunkAttributes);
      chunkAttributes.setNoDictionary(true);
    } else {
      // to store fixed length column chunk values
      columnDataChunk = new FixedLengthDimensionDataChunk(dataPage, chunkAttributes);
      columnDataChunk.setCompleteRleDataChunk(rlePage);
    }
    
/*    end = System.currentTimeMillis();
    if((end -start) >100){
        
       	System.out.println("new columnDataChunk: "+(end -start) + "thread: " + Thread.currentThread().getId());
    }
*/
    rlePage = null;
    return columnDataChunk;
  }

}
