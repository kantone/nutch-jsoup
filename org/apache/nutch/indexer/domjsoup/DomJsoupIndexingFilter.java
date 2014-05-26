/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nutch.indexer.domjsoup;


import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Jsoup dom extractor field extractor for nutch
 * 
 * @author Alex Kantone - @aKantone
 * 
 */
public class DomJsoupIndexingFilter implements IndexingFilter {
  public static final Logger LOG = LoggerFactory.getLogger(DomJsoupIndexingFilter.class);
  public static final String prefix = "_js_";
  private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();

  
 
  @Override
  public NutchDocument filter(NutchDocument doc, String url, WebPage page) throws IndexingException {
	  
	  try{ 
		 LOG.info("DomJsoupIndexingFilter plugin"); 
		 
		 //get page meta
		 Map<Utf8,ByteBuffer> metadata = page.getMetadata();
		
		 //Add to index metadata
		 @SuppressWarnings("rawtypes")
		 Iterator entries = metadata.entrySet().iterator();
		 while(entries.hasNext()) {
			  @SuppressWarnings("rawtypes")
			  Entry thisEntry = (Entry) entries.next();
			  Object key = thisEntry.getKey();
			  Object value = thisEntry.getValue();
			  String cname = value.getClass().getName();
			 // if(value.getClass().equals(java.nio.ByteBuffer.class)){
			  if(cname.equals("java.nio.HeapByteBuffer")){
				  ByteBuffer b = (ByteBuffer)value;
				  String val = new String(b.array());
				  Utf8 keyname = (Utf8)key;
				  
				  if(keyname.toString().startsWith(prefix)){
					 int index =  keyname.toString().indexOf(prefix) + prefix.length();
					 String newKey = keyname.toString().substring(index);
					 
					 
					 if(newKey.endsWith("|@")){
						 newKey = newKey.replace("|@", "");
						 String[] vals = val.split(",");
						 for (String v : vals) {
							 doc.add(newKey, v);
						}
					 }
					 else doc.add(newKey, val);
					 	
					 
					 LOG.info("Added to index field : " + newKey);
				  }
			  }
			  else LOG.info(key + " - " + value);
		  }
	  }
	  catch(Exception ex){
		  LOG.error(ex.getMessage());
	  }
	 
	  return doc;
  }
  
  
    

  private Configuration conf; 
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public Collection<Field> getFields() {
    return FIELDS;
  }
  
  public static void main(String[] args) throws Exception {
	  LOG.info("DomJsoupIndexingFilter plugin");
}


}
