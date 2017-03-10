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

package org.apache.carbondata.core.datastorage.store.columnar;

import java.io.IOException;
import java.util.Arrays;

import org.xerial.snappy.SnappyNew;

public final class UnBlockIndexer {

  private UnBlockIndexer() {

  }

  public static int[] uncompressIndex(int[] indexData, int[] indexMap) {
    int actualSize = indexData.length;
    for (int i = 0; i < indexMap.length; i++) {
      actualSize += indexData[indexMap[i] + 1] - indexData[indexMap[i]] - 1;
    }
    int[] indexes = new int[actualSize];
    int k = 0;
    for (int i = 0; i < indexData.length; i++) {
      int index = Arrays.binarySearch(indexMap, i);
      if (index > -1) {
        for (int j = indexData[indexMap[index]]; j <= indexData[indexMap[index] + 1]; j++) {
          indexes[k] = j;
          k++;
        }
        i++;
      } else {
        indexes[k] = indexData[i];
        k++;
      }
    }
    return indexes;
  }
  
  
  public static int[] uncompressIndex(int[] indexData, int[] indexMap, int limit) {
	    int actualSize = indexData.length;
	    for (int i = 0; i < indexMap.length; i++) {
	      actualSize += indexData[indexMap[i] + 1] - indexData[indexMap[i]] - 1;
	    }
   
/*	    if(limit>0 && limit < actualSize){
	    	
	    	actualSize = limit;
	    }*/
	    
	    int[] indexes = new int[actualSize];
	    int k = 0;

	    for (int i = 0; k<actualSize && i < indexData.length; i++) {
	      int index = Arrays.binarySearch(indexMap, i);
	      if (index > -1) {
	        for (int j = indexData[indexMap[index]]; k<actualSize && j <= indexData[indexMap[index] + 1]; j++) {
	          indexes[k] = j;
	          k++;
	        }
	        i++;
	      } else {
	        indexes[k] = indexData[i];
	        k++;
	      }
	    }
	    return indexes;
	  }

  public static byte[] uncompressData(byte[] data, int[] index, int keyLen) {
    if (index.length < 1) {
      return data;
    }
    int numberOfCopy = 0;
    int actualSize = 0;
    int srcPos = 0;
    int destPos = 0;
    for (int i = 1; i < index.length; i += 2) {
      actualSize += index[i];
    }
    byte[] uncompressedData = new byte[actualSize * keyLen];
    int picIndex = 0;
    for (int i = 0; i < data.length; i += keyLen) {
      numberOfCopy = index[picIndex * 2 + 1];
      picIndex++;
      for (int j = 0; j < numberOfCopy; j++) {
        System.arraycopy(data, srcPos, uncompressedData, destPos, keyLen);
        destPos += keyLen;
      }
      srcPos += keyLen;
    }
    return uncompressedData;
  }

  
  //TODO
  public static byte[] uncompressDataForSort(byte[] data, int[] index, int keyLen, int limit, boolean descSortFlg) {
	  
	  if(descSortFlg){
		return uncompressData(data, index, keyLen);  
	  }
	    if (index.length < 1) {
	      return data;
	    }
	    int numberOfCopy = 0;
	    int srcPos = 0;
	    int destPos = 0;
	    byte[] uncompressedData = new byte[limit * keyLen];
	    int picIndex = 0;
	    for (int i = 0; i < data.length; i += keyLen) {
	      numberOfCopy = index[picIndex * 2 + 1];
	      numberOfCopy = limit < numberOfCopy?limit:numberOfCopy;
	      picIndex++;
	      for (int j = 0; j < numberOfCopy; j++) {
	        System.arraycopy(data, srcPos, uncompressedData, destPos, keyLen);
	        destPos += keyLen;
	      }
	      limit = limit - numberOfCopy;
	      if(limit<=0){
	    	 
	    	  break;
	      }
	      srcPos += keyLen;
	    }
	    return uncompressedData;
	  }
 /* 
  //TODO
  public static byte[] uncompressDataForLimtSort1(byte[] data, int[] index, int keyLen, int limitValue) {
	    if (index.length < 1) {
	      return data;
	    }
	    int numberOfCopy = 0;
	    int srcPos = 0;
	    int destPos = 0;
	    byte[] uncompressedData = new byte[limitValue * keyLen];
	    int picIndex = 0;
	    for (int i = 0; i < data.length; i += keyLen) {
	      numberOfCopy = index[picIndex * 2 + 1];
	      numberOfCopy = limitValue < numberOfCopy?limitValue:numberOfCopy;
	      picIndex++;
	      for (int j = 0; j < numberOfCopy; j++) {
	    	  for(int k = 0; k < numberOfCopy; k++){	    		  
	    		  srcPos ++;	  
	    	  }
	    	  uncompressedData[destPos] = data[srcPos];
	        //System.arraycopy(data, srcPos, uncompressedData, destPos, keyLen);
	        destPos += keyLen;
	      }
	      srcPos += keyLen;
	    }
	    return uncompressedData;
	  }
 
  //TODO
  public static byte[] uncompressDataForLimtSort2(byte[] data, int[] index, int keyLen, int limitValue) {
	    if (index.length < 1) {
	      return data;
	    }
	    int numberOfCopy = 0;
	    int srcPos = 0;
	    int destPos = 0;

	    byte[] uncompressedData = new byte[limitValue * keyLen];
	    int picIndex = 0;
	    for (int i = 0; i < data.length; i += keyLen) {
	      numberOfCopy = index[picIndex * 2 + 1];
	      numberOfCopy = limitValue < numberOfCopy?limitValue:numberOfCopy;
	      picIndex++;
	      for (int j = 0; j < numberOfCopy; j++) {
	        System.arraycopy(data, srcPos, uncompressedData, destPos, keyLen);
	        destPos += keyLen;
	      }
	      srcPos += keyLen;
	    }
	    return uncompressedData;
	  }*/
  
  
  public static  byte[] compressHtml(String html){
      try {
          return SnappyNew.compress(html.getBytes("UTF-8"));
      } catch (IOException e) {
          e.printStackTrace();
          return null;
      }
  }
  
  public static  String decompressHtml(byte[] bytes){
      try {
          return new String(SnappyNew.uncompress(bytes));
      } catch (IOException e) {
          e.printStackTrace();
          return null;
      }
  }
  

  
  public static int getMaxInt(int[] array) {
		
		int max =  array[0];
		for(int i=0;i<array.length;i++)
		  {
			  if(array[i]> max)   // 判断最大值
			  max=array[i];
		  }
		 //System.out.println("getMaxInt: " + max);
		  return max;
	}
  
  public final static void main(String[] args){
	  
	  int repeatCnt = 12000000;
	  //byte[] uncompressedData = new byte[repeatCnt] System.arraycopy(data, srcPos, uncompressedData, destPos, keyLen)
	  
/*	  String testStr = "byte[] uncompressedData = new byte[repeatCnt] System.arraycopy(data, srcPos, uncompressedData, destPos, keyLen)";
	  byte[] compressedData = compressHtml(testStr);
	  System.out.println("decode: " + decompressHtml(compressedData));
	 
	  
	  System.out.println("decode1: " + decompressHtml( Arrays.copyOf(compressedData, 50)));
	  
	  byte[] uncompressedData1 = new byte[repeatCnt];
	  
	 
	  byte[] uncompressedData = new byte[repeatCnt];
	  Byte b = Byte.MIN_VALUE;*/
	 
	  int[] compressedData = new int[repeatCnt];
	  for (int j = 0; j < repeatCnt; j++) {
		  compressedData[j] = j;
	        //System.arraycopy(data, srcPos, uncompressedData, destPos, keyLen);
	        //destPos += keyLen;
	  }

long start = System.currentTimeMillis();
System.out.println("\n数组的最大值是："+getMaxInt(compressedData)); // 输出最大值  

	  
	  
	  long end = System.currentTimeMillis();
	  System.out.println("time: " + (end -start));
	  
	  
	  /* byte[] uncompressedData1 = new byte[repeatCnt];
	  int srcPos = 0;
	  int destPos = 0;
	  start = System.currentTimeMillis();
	  for (int j = 0; j < repeatCnt; j++) {
		  
	        System.arraycopy(uncompressedData, srcPos, uncompressedData1, destPos, 1);
	        destPos++;
	  }
	  end = System.currentTimeMillis();
	  System.out.println("time: " + (end -start));
	  
	  
	  byte[] uncompressedData2 = new byte[repeatCnt];
	  start = System.currentTimeMillis();
	  for (int j = 0; j < repeatCnt; j++) {
		  uncompressedData2[j] = b;
	        //System.arraycopy(data, srcPos, uncompressedData, destPos, keyLen);
	        //destPos += keyLen;
	  }
	  end = System.currentTimeMillis();
	  System.out.println("time: " + (end -start));
	  
	  System.out.println(Arrays.equals(uncompressedData, uncompressedData1));*/
  }


}
