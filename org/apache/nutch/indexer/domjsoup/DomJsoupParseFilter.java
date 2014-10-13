package org.apache.nutch.indexer.domjsoup;

import java.io.File;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.indexer.domjsoup.conf.Rules;
import org.apache.nutch.indexer.domjsoup.rule.Equalcheck;
import org.apache.nutch.indexer.domjsoup.rule.Parse.Fields.Elastic;
import org.apache.nutch.indexer.domjsoup.rule.Parse.Fields.TextProcessAlternative;
import org.apache.nutch.indexer.domjsoup.rule.Parse.Fields.TextProcessAlternativeCheck;
import org.apache.nutch.indexer.domjsoup.rule.TextP.Append;
import org.apache.nutch.indexer.domjsoup.rule.TextP.Contains;
import org.apache.nutch.indexer.domjsoup.rule.TextP.DatePattern;
import org.apache.nutch.indexer.domjsoup.rule.TextP.DatePattern.IndexDef;
import org.apache.nutch.indexer.domjsoup.rule.TextP.PercentCalculate;
import org.apache.nutch.indexer.domjsoup.rule.TextP.Replace;
import org.apache.nutch.indexer.domjsoup.rule.TextP.Split;
import org.apache.nutch.indexer.domjsoup.rule.TextP.Substring;
import org.apache.nutch.indexer.domjsoup.rule.TextP.Trim;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseFilter;
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
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentFragment;

import com.ibm.icu.text.SimpleDateFormat;


/**
 * Jsoup dom extractor field extractor for nutch
 * 
 * @author Alex Kantone - @aKantone
 * 
 */
public class DomJsoupParseFilter implements ParseFilter {
	public static final Logger LOG = LoggerFactory.getLogger(DomJsoupParseFilter.class);
	private Configuration conf; 
	public static final String conffileName = "domjsoupconf.xml";
	public static final String prefix = "_js_";
	private String conffile;
	private org.apache.nutch.indexer.domjsoup.rule.Parse parse  = null;
	private org.apache.nutch.indexer.domjsoup.conf.Rules.Rule rule = null;
	private org.apache.nutch.indexer.domjsoup.conf.Rules.ElasticInfo elasticInfo = null;
	private String currentFieldName ="";
	
	@Override
	public Parse filter(String url, WebPage page, Parse parse,HTMLMetaTags metaTags, DocumentFragment doc) {
		
		LOG.info("DomJsoupParseFilter ParseFilter plugin");
		 try{
						  
			  URL u  = this.conf.getResource(conffileName);
			  this.conffile = u.getPath();
			  LOG.info("conf file : " + this.conffile);
			  		
			  if(page.equals(null)){
				  LOG.error("WebPage is null");
				  return parse;
			  }	  		 
			  
			  ByteBuffer k = page.getContent();		
			  if(k == null){
				  LOG.error("WebPage content is null");
				  return parse;
			  }
			 
			 
			  if(!k.equals(null)){				 
				  String html = new String(k.array(),Charset.forName("UTF-8"));
				  
				  setXpathsRuleFile(url);	 
				  if(!this.rule.equals(null)){
					  
					  if(this.rule.isAddhtmlsourcefield()){	
						  page.putToMetadata(getNewFieldKey("html"),  ByteBuffer.wrap(html.getBytes()));
					  }
					  
					  if(this.parse == null){
						  LOG.error("this.parse=null : cannot load xml rules");
						  return parse;
					  }
					  
					  LOG.info("Parse " + this.parse.getFields().size() + " fields");
					  for (org.apache.nutch.indexer.domjsoup.rule.Parse.Fields entry : this.parse.getFields()) {
						 this.currentFieldName = entry.getFieldname();
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
									  this.put(val, entry, page);
								  }
								  if(elastic.getElasticprocesstype().equals("processFieldJsoup")){
									  String htmlFromElastic = this.elasticQueryByUrl(elastic.getFindUrlValue(),this.elasticInfo.getClusterName(),this.elasticInfo.getHostNameOrIp(),this.elasticInfo.getElasticPort(),this.elasticInfo.getIndex(),elastic.getFieldName());
									  if(htmlFromElastic != null && !htmlFromElastic.isEmpty()){
										  Log.info("Find html into elasticSearch and parse it with this rule");
										  String val = "";
										  Elements el = this.parse(htmlFromElastic, entry.getJsoupquery());
										  if(el != null){											  
											  val= parseRule(el,entry,false,"");
										  }
										  this.put(val, entry, page);
									  }
									  else {
										  Log.warn("Cannot find html value from elasticsearch query");	
										  this.put("", entry, page);
									  }
								  }
							  }
						  }
						
						  
						  if(processStandard){
							  String val = "";
							  
							  //Processing url not perfrom jsoup
							  if(entry.isProcessingUrl()){
								  val= parseRule(null,entry,true,url);
							  }
							  else{
								  //Jsoup processing
								  if(entry.getReturnType().equals("static"))
									  val = entry.getStaticval();
								  else {	
									  if(!entry.getJsoupquery().equals("")){
										  String[] jsoups;
										  if(entry.getJsoupquery().contains("|"))
											  jsoups = entry.getJsoupquery().split("|");
										  else jsoups = new String[1];
										  jsoups[0] = entry.getJsoupquery();
										  
										  //for each query defined perform query and process Text of first query that return not null Element
										  for(int i=0;i<jsoups.length;i++){
											  Elements el = this.parse(html,jsoups[i]);
											  if(el != null){
												  
												  if(entry.isIsMultiple()){													 
													  val= multipleExecution(el,entry,val);
												  }
												  else{													  
													  val= parseRule(el,entry,false,"");
												  }		
												  break;
											  }
										  }	
									  }
								  }
							  }
							  
							  //set default custom value if Empty
							  if(entry.getNothingFoundAction() != null){								 
								  if(entry.getNothingFoundAction().isSetcustomVal()){
									  val = entry.getNothingFoundAction().getCustomVal();
								  }								 
							  }
							  
							  //add array field
							  if(entry.getReturnType().equals("joinfields") ){
								  String joinVal= getJoinFields(entry,page);
								  this.put(joinVal, entry, page);
							  }
							  //add field
							  else {
								  this.put(val, entry, page);
							  }
						  }
					  }
				  }
				  else {
					  LOG.warn("Rule not found or null for url " + url);
				  }
			  }
			  else{
				  LOG.warn("Page content is null : " + url);
			  }
		  }
		  catch(Exception ex){
			  LOG.error("Field : " + this.currentFieldName);
			  LOG.error("Error on url " + url);
			  LOG.error(ex.getMessage());		  
			  String stack = org.apache.commons.lang.exception.ExceptionUtils.getStackTrace(ex);
			  LOG.error(stack);	
			  LOG.error(ex.getLocalizedMessage());			  
		  }	
		
		return parse;
	}
	
	/**
	 * Put to medatada
	 * @param val
	 */
	private WebPage  put(String val,org.apache.nutch.indexer.domjsoup.rule.Parse.Fields entry, WebPage page ){
		if(entry.getConvertToType() != null){
			 if(entry.getConvertToType().getType().equals("empty")){
				 page.putToMetadata(getNewFieldKey(entry.getFieldname()),  ByteBuffer.wrap("".getBytes()));
			 }
			 //check if int else set = 0
			 else if(entry.getConvertToType().getType().equals("integer")){
				 	try{
				 		Integer v = Integer.parseInt(val);
					 	page.putToMetadata(getNewFieldKey(entry.getFieldname()), ByteBuffer.wrap(v.toString().getBytes()));
					 }catch(NumberFormatException e){
						 page.putToMetadata(getNewFieldKey(entry.getFieldname()), ByteBuffer.wrap("0".getBytes()));
					 }				
			 }
			//check if float else set = 0
			 else if(entry.getConvertToType().getType().equals("float")){
				 	try{
				 		Float v = Float.parseFloat(val);
					 	page.putToMetadata(getNewFieldKey(entry.getFieldname()), ByteBuffer.wrap(v.toString().getBytes()));
					 }catch(NumberFormatException e){
						 page.putToMetadata(getNewFieldKey(entry.getFieldname()), ByteBuffer.wrap("0".getBytes()));
					 }				
			 }
			 else if(entry.getConvertToType().getType().equals("date")){	
				 //try parse and set date
				 try {
					SimpleDateFormat parserSDF=new SimpleDateFormat(entry.getConvertToType().getDateFormatToCheck());					 
					Date dt = parserSDF.parse(val);
					SimpleDateFormat toDf=new SimpleDateFormat(entry.getConvertToType().getDateConvertToFormat());					 
					String _data = toDf.format(dt);					
					page.putToMetadata(getNewFieldKey(entry.getFieldname()),  ByteBuffer.wrap(_data.getBytes()));
				} 
				 //Set date default
				 catch (ParseException e) {
					 try{
						 page.putToMetadata(getNewFieldKey(entry.getFieldname()),  ByteBuffer.wrap(entry.getConvertToType().getDefaultDate().getBytes()));
					 }
					 catch(Exception e1){
						 
					 }
				}
			 }
		 }
		else page.putToMetadata(getNewFieldKey(entry.getFieldname()),  ByteBuffer.wrap(val.getBytes()));
		
		return page;
	}
	
	/**
	 * Join fields
	 * @param entry
	 * @param page
	 * @return
	 */
	private String getJoinFields(org.apache.nutch.indexer.domjsoup.rule.Parse.Fields entry,WebPage page){
		 String joinVal = "";
		  try{
			  for (String fieldName : entry.getJoinfields().getFieldName()) {
				  for (Entry<Utf8, ByteBuffer> el : page.getMetadata().entrySet()) {
					if((prefix + fieldName).equals(el.getKey().toString())){
						joinVal +=  new String(el.getValue().array(),Charset.forName("UTF-8")) + entry.getJoinfields().getSeparator();
						break;
					}
				  }				 							 
			  }
			  if(!joinVal.equals("")){
				  joinVal = joinVal.substring(0,joinVal.lastIndexOf(entry.getJoinfields().getSeparator()));										 
			  }			 
		  }
		  catch(Exception ex){
			  LOG.error("Field : " + this.currentFieldName);
			  LOG.error(ex.getMessage());
		  }
		  return joinVal;
	}
		
	
	/**
	 * Execute the rule foreach element find joining it into a string with separator
	 * @param el
	 * @param entry
	 * @param val
	 * @return
	 */
	private String multipleExecution(Elements el,org.apache.nutch.indexer.domjsoup.rule.Parse.Fields entry,String val){
		 for (Element element : el) {	
			  String txt = element.text();
			  
			  if(entry.getReturnType().equals("html"))
				  txt = element.html();
			  
			  if(entry.getReturnType().equals("attr"))
				  txt = element.attr(entry.getAttrname());
			  
			  val= val + parseRule(null,entry,true,txt) + entry.getMultipleSeparator();
		  }
		 return val;
	}
	
	/**
	 * Generate key with prefix
	 * @param name
	 * @return
	 */
	private Utf8 getNewFieldKey(String name){
		return new Utf8(prefix + name);
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
	  private String parseRule(Elements el,org.apache.nutch.indexer.domjsoup.rule.Parse.Fields rule,Boolean useText,String text){	  
		  String val = "";
		  
		  //Move to parent
		  if(rule.getMoveToParent() != null){
			  if(rule.getMoveToParent().isEnabled()){	
				 if(el.parents().size() >= rule.getMoveToParent().getGoBackParentsNumbers()){
					 Element el2 =  el.parents().get(rule.getMoveToParent().getGoBackParentsNumbers());
					 el = this.parse(el2.html(),rule.getMoveToParent().getJsoupquery());
				 }
			  }			  
		  }
		  
		  if(useText.equals(false)){
			  //set value
			  if(rule.getReturnType().equals("text") || rule.getReturnType().equals("array"))
				  val = el.text();
			  else if (rule.getReturnType().equals("html"))
				  val = el.html();
			  else if (rule.getReturnType().equals("attr"))
				  val = el.attr(rule.getAttrname());
			  else if (rule.getReturnType().equals("count"))
				  val = String.valueOf(el.size());
			  
			
		  }
		  else{
			  val = text;
		  }
		  
		 
		  //override val by equalcheck	
		  if(rule.getEqualcheckBeforeTextProcess() != null){
			  val = this.equalNotEqualCheck(val,rule.getEqualcheckBeforeTextProcess());
		  }
		 
		  //Check if load text process alternative if val contains
		  org.apache.nutch.indexer.domjsoup.rule.TextP textProcess  = this.getTextProcess(val, rule);
   
  
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
			  
			  //Contains
			  Contains cont = textProcess.getContains();
			  if(cont != null){				  
				  if(cont.isCaseInsensitive()){
					  if(val.toLowerCase().contains(cont.getContainValue().toLowerCase())){
						  val = cont.getCustomValue();
					  }
				  }
				  else {
					  if(val.contains(cont.getContainValue())){
						  val = cont.getCustomValue();
					  }
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
				  if(substr.getType().toLowerCase().equals("beginindex")){
					  val = val.substring(substr.getBeginindex());
				  }
				  if(substr.getType().toLowerCase().equals("fromto")){
					  val = val.substring(substr.getBeginindex(),substr.getEndindex());
				  }
			  }
			  
			  //Split
			  val = split(val,textProcess);
			  
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
			  
			  
			  //percentCalculator
			  PercentCalculate percent = textProcess.getPercentCalculate();
			  if(percent != null){
				  try{
					  int val2 = Integer.parseInt(val);
					  int res = (100 * val2)/percent.getTotalValue();
					  val = String.valueOf(res);
				  }
				  catch(Exception e){
					  LOG.error("Field : " + this.currentFieldName);
					  LOG.error(e.getMessage() + " " + e.getStackTrace());
				  }
				  
				  if(percent.isAppendPercentValue()){
					  val = val + "%";
				  }
			  }
			  			 
			  val = datePattern(val,textProcess);
			  			
			  //Trim
			  List<Trim> trim = textProcess.getTrim();
			  if(trim != null){
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
	   * Find an alternative text Process else return default
	   * @param val
	   * @param textProcess
	   * @param rule
	   * @return
	   */
	  private org.apache.nutch.indexer.domjsoup.rule.TextP getTextProcess(String val,org.apache.nutch.indexer.domjsoup.rule.Parse.Fields rule){
		  org.apache.nutch.indexer.domjsoup.rule.TextP textProcess = rule.getTextProcess();
		  if(rule.getTextProcessAlternativeCheck() != null){
			  for (TextProcessAlternativeCheck t : rule.getTextProcessAlternativeCheck()) {
				boolean brk = false;
				if(t.isEnable() && val.contains(t.getContainsVal())){
					if(rule.getTextProcessAlternative() != null){
						for(TextProcessAlternative a : rule.getTextProcessAlternative()){
							if(a.getId().equals(t.getReferenceId())){
								textProcess = a.getTextProcess();
								brk = true;
								break;								
							}
						}
					}	
				}
				if(brk)
					break;
			}
		  }
		  return textProcess;
	  }
	  
	  /**
	   * Execute date convertion pattern
	   * @param val
	   * @param textProcess
	   * @return
	   */
	  private String datePattern(String val, org.apache.nutch.indexer.domjsoup.rule.TextP textProcess){
		  DatePattern dtp = textProcess.getDatePattern();
		  if(dtp != null && !val.equals("")){
			  String d="";
			  String M = "";
			  String y = "";
			  String k= "";
			  String m ="";
			  String s ="";
			  try{
				  String[] explode = val.split(dtp.getSeparator());
				  for (IndexDef def : dtp.getIndexDef()) {
					String p = explode[def.getIndex()].trim();
					String letter = def.getDateLetter();
					if(letter.equals("d"))
						d = p;
					else if(letter.equals("y"))
						y = p;
					else if(letter.equals("M"))
						M = p;
					else if(letter.equals("k"))
						k = p;
					else if(letter.equals("m"))
						m = p;
					else if(letter.equals("s"))
						s = p;
				  }
				  
				  
				  SimpleDateFormat dt = new SimpleDateFormat(dtp.getDateFormat());				  
				  if(dtp.getType().equals("splitPattern")){
					  
					  //set defaults
					  if(d.equals(""))
						  d = "1";
					  if(M.equals(""))
						  M = "1";
					  if(y.equals(""))
						  y = "1970";
					  if(k.equals(""))
						  k = "00";
					  if(m.equals(""))
						  m = "00";
					  if(s.equals(""))
						  s = "00";
					  
					  GregorianCalendar gr = new GregorianCalendar(Integer.parseInt(y.trim()),Integer.parseInt(M.trim()),Integer.parseInt(d.trim()),Integer.parseInt(k.trim()),Integer.parseInt(m.trim()),Integer.parseInt(s.trim()));
					  val = dt.format(gr.getTime());
				  }
				  else if(dtp.getType().equals("splitPatternFromNow")){
					  Calendar gr =  GregorianCalendar.getInstance();
					  
					  //set defaults
					  if(d.equals(""))
						  d = "0";
					  if(M.equals(""))
						  M = "0";
					  if(y.equals(""))
						  y = "0";
					  if(k.equals(""))
						  k = "0";
					  if(m.equals(""))
						  m = "0";
					  if(s.equals(""))
						  s = "0";
					  
					  
					  int c = 1;
					  if(dtp.isSplitPatternFromNowSubtract()){
						  c=-1;						  
					  }
					  
					  if(!d.equals(""))
						  gr.add(Calendar.DAY_OF_MONTH,Integer.parseInt(d)*c);
					  
					  if(!M.equals(""))
						  gr.add(Calendar.MONTH,Integer.parseInt(M)*c);
					  
					  if(!y.equals(""))
						  gr.add(Calendar.YEAR,Integer.parseInt(y)*c);
					  
					  if(!k.equals(""))
						  gr.add(Calendar.HOUR,Integer.parseInt(k)*c);
					  
					  if(!m.equals(""))
						  gr.add(Calendar.MINUTE,Integer.parseInt(m)*c);
					  
					  if(!s.equals(""))
						  gr.add(Calendar.SECOND,Integer.parseInt(s)*c);
				  
					  
					  val =  dt.format(gr.getTime());
				  }
				  
				
			  }
			  catch(Exception e){
				  LOG.error("Field : " + this.currentFieldName);
				  LOG.error(e.getMessage());
				  val = "";
			  }
		  }
		  return val;
	  }
	  
	  /**
	   * Split rule
	   * @param val
	   * @param textProcess
	   * @return
	   */
	  private String split(String val,org.apache.nutch.indexer.domjsoup.rule.TextP textProcess){		 
		  try{
			  List<Split> splits = textProcess.getSplit();
			  for (Split split : splits) {
				  if(split != null){
					  
					  if(!val.equals("")){
						  String[] vals = val.split(split.getSplitvalue());
						  
						  if(split.getType().equals("first")){
							  val = vals[0];
						  }
						  if(split.getType().equals("last")){
							  val = vals[vals.length-1];
						  }
						  if(split.getType().equals("index")){						  
						  
							  
							  if(split.getReturnindex() != null){
								  //mulpiple values
								  if(split.getReturnindex().size() > 1){	
									  String val2 = "";
									  for (int i : split.getReturnindex()) {
										  val2 += vals[i] + split.getSeparator();
									  }
									  val = val2;
								  }
								  else {
									  //single value
									  int index = split.getReturnindex().get(0);
									  val = vals[index];
								  }
							  }
						  }
					  }
				  }
			  }
		  }
		  catch(Exception ex){
			  LOG.error("Field : " + this.currentFieldName);
			  LOG.error(ex.getMessage());
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
		  else if (e.getType().equals("lengthGT")){
			  if(val.length() > e.getLengthtocheck()){
				  val = e.getReplaceval();
			  }
		  }	 
		  else if (e.getType().equals("lengthLT")){
			  if(val.length() < e.getLengthtocheck()){
				  val = e.getReplaceval();
			  }
		  }	 
		  return val;
	  }
	  
	  
	  /**
	   * Perform Jsoup
	   * @param html
	   */
	  private Elements parse(String html,String query){
		  					  
			    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			    factory.setNamespaceAware(false);
			    factory.setValidating(false);
			    try {	    		
			    	
			    	org.jsoup.nodes.Document doc2  = Jsoup.parse(html);		  
			    	if(!query.equals("")){
			    		Elements viewedEl = doc2.select(query);	
			    		return viewedEl;
			    	}
			    	else return null;
			    	
				} 
				catch (Exception e) {
					LOG.error("Field : " + this.currentFieldName);
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
			LOG.error("Field : " + this.currentFieldName);
			LOG.error(e.getMessage());
			e.printStackTrace();
			return;
		}	  
	  }
	

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;		
	}
	
	@Override
	public Collection<Field> getFields() {
		return null;
	}

	

	

}
