//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, vJAXB 2.1.10 in JDK 6 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2014.06.17 at 11:57:57 AM CEST 
//


package org.apache.nutch.indexer.domjsoup.rule;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;


/**
 * <p>Java class for equalcheck complex type.
 * 
 * <p>The following schema fragment specifies the expected content contained within this class.
 * 
 * <pre>
 * &lt;complexType name="equalcheck">
 *   &lt;complexContent>
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType">
 *       &lt;sequence>
 *         &lt;element name="lengthtocheck" type="{http://www.w3.org/2001/XMLSchema}int"/>
 *         &lt;element name="valtocheck" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="replaceval" type="{http://www.w3.org/2001/XMLSchema}string"/>
 *         &lt;element name="type">
 *           &lt;simpleType>
 *             &lt;restriction base="{http://www.w3.org/2001/XMLSchema}string">
 *               &lt;enumeration value="equal"/>
 *               &lt;enumeration value="notequal"/>
 *               &lt;enumeration value="lengthGT"/>
 *               &lt;enumeration value="lengthLT"/>
 *             &lt;/restriction>
 *           &lt;/simpleType>
 *         &lt;/element>
 *       &lt;/sequence>
 *     &lt;/restriction>
 *   &lt;/complexContent>
 * &lt;/complexType>
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "equalcheck", propOrder = {
    "lengthtocheck",
    "valtocheck",
    "replaceval",
    "type"
})
public class Equalcheck {

    protected int lengthtocheck;
    @XmlElement(required = true)
    protected String valtocheck;
    @XmlElement(required = true)
    protected String replaceval;
    @XmlElement(required = true)
    protected String type;

    /**
     * Gets the value of the lengthtocheck property.
     * 
     */
    public int getLengthtocheck() {
        return lengthtocheck;
    }

    /**
     * Sets the value of the lengthtocheck property.
     * 
     */
    public void setLengthtocheck(int value) {
        this.lengthtocheck = value;
    }

    /**
     * Gets the value of the valtocheck property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getValtocheck() {
        return valtocheck;
    }

    /**
     * Sets the value of the valtocheck property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setValtocheck(String value) {
        this.valtocheck = value;
    }

    /**
     * Gets the value of the replaceval property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getReplaceval() {
        return replaceval;
    }

    /**
     * Sets the value of the replaceval property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setReplaceval(String value) {
        this.replaceval = value;
    }

    /**
     * Gets the value of the type property.
     * 
     * @return
     *     possible object is
     *     {@link String }
     *     
     */
    public String getType() {
        return type;
    }

    /**
     * Sets the value of the type property.
     * 
     * @param value
     *     allowed object is
     *     {@link String }
     *     
     */
    public void setType(String value) {
        this.type = value;
    }

}
