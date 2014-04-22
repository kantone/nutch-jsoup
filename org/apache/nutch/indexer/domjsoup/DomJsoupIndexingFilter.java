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


import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.IndexingException;
import org.apache.nutch.indexer.IndexingFilter;
import org.apache.nutch.indexer.NutchDocument;
import org.apache.nutch.indexer.domjsoup.conf.Rules;
import org.apache.nutch.indexer.domjsoup.rule.Equalcheck;
import org.apache.nutch.indexer.domjsoup.rule.Parse.Fields.Elastic;
import org.apache.nutch.indexer.domjsoup.rule.Parse.Fields.TextProcess.Append;
import org.apache.nutch.indexer.domjsoup.rule.Parse.Fields.TextProcess.Replace;
import org.apache.nutch.indexer.domjsoup.rule.Parse.Fields.TextProcess.Split;
import org.apache.nutch.indexer.domjsoup.rule.Parse.Fields.TextProcess.Substring;
import org.apache.nutch.indexer.domjsoup.rule.Parse.Fields.TextProcess.Trim;
import org.apache.nutch.storage.WebPage;
import org.apache.nutch.storage.WebPage.Field;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.mortbay.log.Log;
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
  public static final String conffileName = "domjsoupconf.xml";
  private String conffile;
  private static Collection<WebPage.Field> FIELDS = new HashSet<WebPage.Field>();
  private org.apache.nutch.indexer.domjsoup.rule.Parse parse  = null;
  private org.apache.nutch.indexer.domjsoup.conf.Rules.Rule rule = null;
  private org.apache.nutch.indexer.domjsoup.conf.Rules.ElasticInfo elasticInfo = null;
  @Override
  public NutchDocument filter(NutchDocument doc, String url, WebPage page) throws IndexingException {
	  

	  try{
	  URL u  = this.conf.getResource(conffileName);
	  this.conffile = u.getPath();
	  LOG.info("conf file : " + this.conffile);
	  ByteBuffer k = page.getContent();
	  String html = new String(k.array(),Charset.forName("UTF-8"));
	  
	  setXpathsRuleFile(url);	 
	  if(this.rule != null){
		  
		  if(this.rule.isAddhtmlsourcefield()){			
			  doc.add("html", html);
		  }
		  
		  if(this.parse == null){
			  LOG.error("this.parse=null : cannot load xml rules");
			  return doc;
		  }
		  
		  LOG.info("Parse " + this.parse.getFields().size() + " fields");
		  for (org.apache.nutch.indexer.domjsoup.rule.Parse.Fields entry : this.parse.getFields()) {
			
			  boolean processStandard = true;
			  
			  //ElasticCase
			  Elastic elastic = entry.getElastic();
			  if(elastic != null){
				  if(elastic.isFindintoelastic()){
					  processStandard = false;
					  
					  //Set this field as elasticSearch
					  if(elastic.getElasticprocesstype().equals("setFieldValue")){
						  String val = this.elasticQueryByUrl(elastic.getFindUrlValue(),this.elasticInfo.getClusterName(),this.elasticInfo.getHostNameOrIp(),this.elasticInfo.getElasticPort(),this.elasticInfo.getIndex(),elastic.getFieldName());
						  Log.info("Setting " + entry.getFieldname() + " from elasticSearch query result");
						  doc.add(entry.getFieldname(), val);
					  }
					  if(elastic.getElasticprocesstype().equals("processFieldJsoup")){
						  String htmlFromElastic = this.elasticQueryByUrl(elastic.getFindUrlValue(),this.elasticInfo.getClusterName(),this.elasticInfo.getHostNameOrIp(),this.elasticInfo.getElasticPort(),this.elasticInfo.getIndex(),elastic.getFieldName());
						  if(htmlFromElastic != null && !htmlFromElastic.isEmpty()){
							  Log.info("Find html into elasticSearch and parse it with this rule");
							  String val = "";
							  Elements el = this.parse(htmlFromElastic, entry.getJsoupquery());
							  if(el != null){
								  val= parseRule(el,entry);
							  }
							  doc.add(entry.getFieldname(), val);
						  }
						  else {
							  Log.warn("Cannot find html value from elasticsearch query");
							  doc.add(entry.getFieldname(), "");
						  }
					  }
				  }
			  }
			  
			  if(processStandard){
				  String val = "";
				  Elements el = this.parse(html, entry.getJsoupquery());
				  if(el != null){
					  val= parseRule(el,entry);
				  }
				  doc.add(entry.getFieldname(), val);
			  }
		  }
	  }
	  }
	  catch(Exception ex){
		  LOG.error(ex.getMessage());
		  String stack = org.apache.commons.lang.exception.ExceptionUtils.getStackTrace(ex);
		  LOG.error(stack);		 
	  }
	  
	 
	  return doc;
  }
  
  /**
   * Query ElasticSearch for find field value or html to process
   * @param url
   * @param clusterName
   * @param hostNameOrIp
   * @param hostPortOrIp
   * @param index
   * @param fieldAsString
   * @return
   */
  private String elasticQueryByUrl(String url,String clusterName,String hostNameOrIp,int hostPort,String index,String fieldAsString){	
	  Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).build();
	  TransportClient client = new TransportClient(settings);
	  client.addTransportAddress(new InetSocketTransportAddress(hostNameOrIp, hostPort));
	  QueryBuilder q =QueryBuilders.commonTerms("url", url);
	 
	  SearchResponse response = client.prepareSearch(index)
			  .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
			  .setQuery(q)
			  .setFrom(0).setSize(1)
			  .setExplain(true)
			  .execute().actionGet();
	  	 
	  SearchHits hits = response.getHits();
	  if(hits.totalHits() >= 1){				
		  SearchHit hit = hits.getAt(0);
		  Map<String, Object> s = hit.sourceAsMap();
		  String o =(String) s.get(fieldAsString);		
		  return o;
	  }
	  return null;
  }
  
  
  /***
   * Process rule xml 
   * @param el
   * @param rule
   * @return
   */
  private String parseRule(Elements el,org.apache.nutch.indexer.domjsoup.rule.Parse.Fields rule){	  
	  String val = "";
	  
	  //set value
	  if(rule.getReturnType().equals("text"))
		  val = el.text();
	  else if (rule.getReturnType().equals("html"))
		  val = el.html();
	  else if (rule.getReturnType().equals("attr"))
		  val = el.attr(rule.getAttrname());
	  else if (rule.getReturnType().equals("count"))
		  val = String.valueOf(el.size());
	  else if(rule.getReturnType().equals("static")){
		  return rule.getStaticval();
	  }
	  
	 
	  //override val by equalcheck	
	  if(rule.getEqualcheckBeforeTextProcess() != null){
		  val = this.equalNotEqualCheck(val,rule.getEqualcheckBeforeTextProcess());
	  }
	 
	  
	  org.apache.nutch.indexer.domjsoup.rule.Parse.Fields.TextProcess textProcess =  rule.getTextProcess();	  
	  if(textProcess != null){
		 
		  //regex
		  if(textProcess.getRegex() != null){
			  if(!textProcess.getRegex().equals("")){
				  Pattern pattern = Pattern.compile(textProcess.getRegex());
				  Matcher match = pattern.matcher(val);
				  match.find();
				  val = match.group();
			  }
		  }
		  
		  //Replace
		  List<Replace> repl = textProcess.getReplace();
		  if(repl != null){
			  for(Replace r : repl){
				  val = val.replace(r.getFindstring(), r.getVal());
			  }
		  }
		  
		  //Substring
		  Substring substr = textProcess.getSubstring();
		  if(substr != null){
			  if(substr.getType().equals("beginindex")){
				  val = val.substring(substr.getBeginindex());
			  }
			  if(substr.getType().equals("fromto")){
				  val = val.substring(substr.getBeginindex(),substr.getEndindex());
			  }
		  }
		  
		  //Split
		  Split split = textProcess.getSplit();
		  if(split != null){
			  
			  String[] vals = val.split(split.getSplitvalue());
			  if(split.getReturnindex() != null){
				  //mulpiple values
				  if(split.getReturnindex().size() > 1){	
					  String val2 = "";
					  for (int i : split.getReturnindex()) {
						  val2 += vals[i] + split.getSeparator();
					  }
					  val = val2;
				  }
				  else{
					  //single value
					  int index = split.getReturnindex().get(0);
					  val = vals[index];
				  }
			  }
		  }		  
		
		  //append
		  List<Append> append = textProcess.getAppend();
		  if(append != null){
			  for(Append a : append){
				  if(a.getType().equals("before") || a.getType().equals("both")){
					  val = a.getVal() + val;
				  }
				  if(a.getType().equals("after") || a.getType().equals("both")){
					  val =  val + a.getVal();
				  }
			  }
		  }
		  
		//post process text
		  //Trim
		  List<Trim> trim = textProcess.getTrim();
		  if(split != null){
			  for(Trim t : trim){
				  if(t.getType().equals("left"))
					  val = val.replaceAll("^" + t.getTrimvalue(), "");
				  else if(t.getType().equals("right"))
					  val = val.replaceAll( t.getTrimvalue() + "$","");
				  else if(t.getType().equals("both")){
					  val = val.replaceAll("^" + t.getTrimvalue(), "");
					  val = val.replaceAll( t.getTrimvalue() + "$","");
				  }		 
			  }
		  }
		  
	  }
	  
	  
	  if(rule.getEqualcheckAfterTextProcess() != null){
		  val = this.equalNotEqualCheck(val,rule.getEqualcheckAfterTextProcess());
	  }
	  
	  return val;
  }
  
 /**
  * Override value if equal or not
  * @param val
  * @param e
  * @return
  */
private String equalNotEqualCheck(String val,Equalcheck e){	
	  if(e.getType().equals("equal")){
		  if(val.equals(e.getValtocheck())){
			  val = e.getReplaceval();
		  }
	  }
	  else if (e.getType().equals("notequal")){
		  if(!val.equals(e.getValtocheck())){
			  val = e.getReplaceval();
		  }
	  }	 
	  return val;
  }
  
  
  /**
   * Perform xpath
   * @param html
   */
  private Elements parse(String html,String query){
		//My Xpath
		  
		    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		    factory.setNamespaceAware(false);
		    factory.setValidating(false);
		    try {	    		
		    	
		    	org.jsoup.nodes.Document doc2  = Jsoup.parse(html);		    	
		    	Elements viewedEl = doc2.select(query);
		    	return viewedEl;
			} 
			catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		  
	  }

  /**
   * Read global xpath configuration file and return the right xpath rules xml files by url contains filter
   * @return
   */
  private void setXpathsRuleFile(String url){
	  try {
		  File f = new File(conffile);
		  if(f.exists()){
			  LOG.info("Load " + conffile);
			  JAXBContext jaxbContext = JAXBContext.newInstance(Rules.class);
			  Unmarshaller jaxbUnmarshaller = jaxbContext.createUnmarshaller();		  
			  Rules config = (Rules) jaxbUnmarshaller.unmarshal(f);
			  this.elasticInfo = config.getElasticInfo();
			  
			  for (org.apache.nutch.indexer.domjsoup.conf.Rules.Rule entry : config.getRule()) {
					
				  this.rule = entry;
				  
				  //Perform Xpath here
				  if(url.contains(entry.getUrlcontain())){
					  File f2 = new File(entry.getFile());
					  if(f2.exists()){
						  LOG.info("Loading " + entry.getFile());
						  jaxbContext = JAXBContext.newInstance( org.apache.nutch.indexer.domjsoup.rule.Parse.class);
						  jaxbUnmarshaller = jaxbContext.createUnmarshaller();		  
						  this.parse = (org.apache.nutch.indexer.domjsoup.rule.Parse) jaxbUnmarshaller.unmarshal(f2);
						  return;
					  }
					  else{						  
						  LOG.error("File not exist : " + entry.getFile());
					  }
				  }	  
			  }		  
		  }
		  else{
			  LOG.error("File not exist : " + conffile);
		  }
		  return;
		  

	} catch (JAXBException e) {
		e.printStackTrace();
		return;
	}	  
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
	  System.out.println("DomJsoupIndexingFilter plugin");
}


}
