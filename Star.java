package edu.easternct.bigdata;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

/** This class manages a star object. It allows you to manage and retrieve 
 * the data needed to be stored about a star
*/
	
public class Star implements Serializable, Comparable<Star>{
/**
	 * 
	 */
	private static final long serialVersionUID = 8332314307058700472L;

	/**
	 * 
	 */
	
//	private static final long serialVersionUID = 8332314307058700472L;
	private String tychoId;
	private String meanFlag;
	private Double raMdeg;
	private Double deMdeg;
	private BigDecimal btMag;
	private BigDecimal vtMag;
	private String hipNum;
	private BigDecimal bMinV;
	private Integer raBin;
	private Integer colorInd;
	private Double v;
	
	/** This method is the default constructor for the star class
	 *  @param it takes in no data
	 *  @return it returns no data*/
	public Star() {
		this.tychoId = "";
		this.raMdeg = 0.0;
		this.deMdeg = 0.0;
		this.btMag = new BigDecimal(0.0);
		this.vtMag = new BigDecimal(0.0);
		this.hipNum = "";
		this.bMinV = new BigDecimal(0.0);
		
	}
	/** This method is the constructor for the star class
	 *
	 */
	public Star(String tychoId, String meanFlag, Double raMdeg, Double deMdeg, BigDecimal btMag, BigDecimal vtMag, String hipNum) {
		this.tychoId = tychoId;
		this.meanFlag = meanFlag;
		this.raMdeg = raMdeg;
		this.deMdeg = deMdeg;
		this.btMag = btMag;
		this.vtMag = vtMag;
		this.hipNum = hipNum;
		this.bMinV = new BigDecimal(0.0);
	}
	
	/**
	 * @return the tychoId
	 */
	public String getTychoId() {
		return tychoId;
	}
	/**
	 * @param tychoId the tychoId to set
	 */
	public void setTychoId(String tychoId) {
		this.tychoId = tychoId;
	}
	
	/**
	 * @return the meanFlag
	 */
	public String getMeanFlag() {
		return meanFlag;
	}
	/**
	 * @param meanFlag the meanFlag to set
	 */
	public void setMeanFlag(String meanFlag) {
		this.meanFlag = meanFlag;
	}
	/**
	 * @return the raMdeg
	 */
	public Double getRaMdeg() {
		return raMdeg;
	}
	/**
	 * @param raMdeg the raMdeg to set
	 */
	public void setRaMdeg(Double raMdeg) {
		this.raMdeg = raMdeg;
	}
	/**
	 * @return the deMdeg
	 */
	public Double getDeMdeg() {
		return deMdeg;
	}
	/**
	 * @param deMdeg the deMdeg to set
	 */
	public void setDeMdeg(Double deMdeg) {
		this.deMdeg = deMdeg;
	}
	/**
	 * @return the btMag
	 */
	public BigDecimal getBtMag() {
		return btMag;
	}
	/**
	 * @param btMag the btMag to set
	 */
	public void setBtMag(BigDecimal btMag) {
		this.btMag = btMag;
	}
	/**
	 * @return the vtMag
	 */
	public BigDecimal getVtMag() {
		return vtMag;
	}
	/**
	 * @param vtMag the vtMag to set
	 */
	public void setVtMag(BigDecimal vtMag) {
		this.vtMag = vtMag;
	}
	/**
	 * @return the hipNum
	 */
	public String getHipNum() {
		return hipNum;
	}
	/**
	 * @param hipNum the hipNum to set
	 */
	public void setHipNum(String hipNum) {
		this.hipNum = hipNum;
	}
	/**
	 * @return the bMinV
	 */
	public BigDecimal getBMinV() {
		return bMinV;
	}
	/**
	 * @param bMinV the bMinV to set
	 */
	public void setBMinV(BigDecimal bMinV) {
		this.bMinV = bMinV;
	}

	/**
	 * @param none
	 */
	public void setBMinV() {
		
		this.bMinV = btMag.subtract(vtMag,new MathContext(6)).multiply(new BigDecimal(.850),new MathContext(6));
		
		double temp = bMinV.doubleValue();
		if (temp >= 1.4) colorInd = 5;
		else if (temp >= .82) colorInd = 4;
		else if (temp >= .31) colorInd = 3;
		else if (temp >= .00) colorInd = 2;
		else colorInd = 1;
		
//		v = vtMag - .090 * (btMag - vtMag);
	}
	
	/**
	 * @return the raBin
	 */
	public Integer getRaBin() {
		return raBin;
	}

	/**
	 * @param raBin the raBin to set
	 */
	public void setRaBin(int raBin) {
		
		  this.raBin = raBin;
	}

	
	
	/**
	 * @param raBin the raBin to set
	 */
	public void setRaBin() {
		
		if (deMdeg > 0)
		{	
		  Double tempRaBin = raMdeg/1;
		  Double tempLeftover = raMdeg % 1;
		  if (tempLeftover > 0)
		      this.raBin = tempRaBin.intValue() + 1;
		  else
			  this.raBin = tempRaBin.intValue();
		}
		else
			raBin = 0;
	}
	
	
	/**
	 * @return the colorInd
	 */
	public Integer getColorInd() {
		return colorInd;
	}
	/**
	 * @param colorInd the colorInd to set
	 */
	public void setColorInd(Integer colorInd) {
		this.colorInd = colorInd;
	}
	
	
	/**
	 * @return the v
	 */
	public Double getV() {
		return v;
	}
	/**
	 * @param v the v to set
	 */
	public void setV(Double v) {
		this.v = v;
	}
	/** This method compares ther passed Person object with the called.
	 *  @param a person object
	 *  @returns 0, if the the two are equal
	 *   returns -1, if the passed is greater
	 *   returns 1, if the passed is less*/
	public int compareTo(Star star) {
		
		if (this == star)
			return 0;
		
		return this.tychoId.compareTo(star.tychoId);
		
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((tychoId == null) ? 0 : tychoId.hashCode());
		return result;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Star other = (Star) obj;
		if (tychoId == null) {
			if (other.tychoId != null)
				return false;
		} else if (!tychoId.equals(other.tychoId))
			return false;
		return true;
	}

	/** This method returns a String display of the person's instance variables.
	 *  @param it receives no input
	 *  @return personData it returns a string of data*/
	public String toFormattedString() {
		return ("Id: " + getTychoId() + " RA Mag Degree: " + getRaMdeg() + "De Mag Degree: " + getDeMdeg() + " " + " BT Mag: " + getBtMag() +
				" " + " VT Mag: " + " " + getVtMag() + " Hip Num: " + " " + getHipNum() + " B-V: " + " " + getBMinV());
	}
	
	public String toString() {
		
		
		StringBuffer tempString = new StringBuffer(100);
		
		tempString.append(this.getTychoId() + ",");
		tempString.append(this.getRaMdeg() + ",");
		tempString.append(this.getDeMdeg() + ",");
		tempString.append(this.getBtMag() + ",");
		tempString.append(this.getVtMag() + ",");
		tempString.append(this.getHipNum() + ",");
		tempString.append(this.getBMinV() + ",");
		tempString.append(this.getRaBin());
		
	    return tempString.toString();
		
	}
}
