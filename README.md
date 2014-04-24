nutch-jsoup
===========
Precise data extraction with nutch and Jsoup css selector via xml configuration

- Extract data with super easy jsoup selector ( jquery like )
```sh
Example : #nodeId tr:nth-child(2) td:nth-child(2) p 
```

- Add custom field for each specific data that you need
- ElasticSearch query integration

Compatibility
----
Compatible with nutch 2.2.1

Intallation
----
1. Copy "plugin/index-domsjoup" under plugin directory
2. Copy "conf/domjsoupconf.xml" under conf directory
3. Copy and edit "conf/example.xml" under conf directory


Configuration
----
- Add "index-domsjoup" to your plugin.includes property in nutch-site.xml
```sh
<property>
 <name>plugin.includes</name>
 <value>protocol-http|urlfilter-regex|parse-(html|tika)|index-(basic|anchor)|index-domsjoup|urlnormalizer-(pass|regex|basic)|scoring-opic</value>
 <description></description>
</property>
```
- Edit conf/domjsoupconf.xml
- Setup filter url where to apply precise data extraction (add or edit rule a section)
```sh
<rule>
    <!-- If url contains "urlcontain value" then use specific file for extract data "conf/domjsoup-github.xml" -->
	<urlcontain>http://github.com</urlcontain>
	<file>conf/domjsoup-github.xml</file>    
	<!-- true : add a field with html source -->
	<addhtmlsourcefield>true</addhtmlsourcefield>    
</rule>
```
- Create domjsoup-github.xml or copy "conf/example.xml" and edit it
```sh
See comments in "conf/example.xml"
```

How to test
----
You can test it using "indexchecker" COMMAND
```sh
bin/nutch indexchecker http://yourUrl.html
<<<<<<< HEAD
```
=======
```
>>>>>>> e433e059d51fce0615ce8898cc3fd5daeb6f72ae
