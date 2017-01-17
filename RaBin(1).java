package edu.easternct.bigdata;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;

/** This class manages a star object. It allows you to manage and retrieve 
 * the data needed to be stored about a star
*/
	
public class RaBin implements Serializable, Comparable<RaBin>{
/**
	 * 
	 */
	private static final long serialVersionUID = 8332314307058700472L;

	/**
	 * 
	 */
	
//	private static final long serialVersionUID = 8332314307058700472L;
	private Integer binNum;
	private Integer starCount;
	private BigDecimal totBMinV;
	private BigDecimal avgBMinV;
	private BigDecimal median;
	private BigDecimal mean;
	private BigDecimal standD;
	private BigDecimal minumum;
	private BigDecimal maximum;
	private Integer spectralOcount;
	private Integer spectralBcount;
	private Integer spectralAcount;
	private Integer spectralFcount;
	private Integer spectralGcount;
	private Integer spectralKcount;
	private Integer spectralMcount;
		
	/** This method is the default constructor for the star class
	 *  @param it takes in no data 
	 *  @return it returns no data*/
	public RaBin() {
		this.binNum = 0;
		this.starCount = 0;
		this.avgBMinV = new BigDecimal("0.0");
		this.totBMinV = new BigDecimal("0.0");
		this.mean = new BigDecimal("0.0");
		this.median = new BigDecimal("0.0");
		this.standD = new BigDecimal("0.0");
		this.maximum = new BigDecimal("0.0");
		this.minumum = new BigDecimal("0.0");
		this.spectralOcount = 0;
		this.spectralBcount = 0;
		this.spectralAcount = 0;
		this.spectralFcount = 0;
		this.spectralGcount = 0;
		this.spectralKcount = 0;
		this.spectralMcount = 0;
				
	}
	/** This method is the constructor for the star class
	 *
	 */
	public RaBin(Integer binNum, Integer starCount, BigDecimal totBMinV) {
		this.binNum = binNum;
		this.starCount = starCount;
		this.totBMinV = totBMinV;
		if (starCount > 0)
			avgBMinV = totBMinV.divide(new BigDecimal(starCount),6,RoundingMode.HALF_UP);
		else
			this.avgBMinV = new BigDecimal("0.0");
		this.mean = new BigDecimal("0.0");
		this.median = new BigDecimal("0.0");
		this.standD = new BigDecimal("0.0");
		this.maximum = new BigDecimal("0.0");
		this.minumum = new BigDecimal("0.0");
		this.spectralOcount = 0;
		this.spectralBcount = 0;
		this.spectralAcount = 0;
		this.spectralFcount = 0;
		this.spectralGcount = 0;
		this.spectralKcount = 0;
		this.spectralMcount = 0;
	}
	
	public RaBin(Integer binNum, Integer starCount, BigDecimal totBMinV, BigDecimal mean, BigDecimal median, 
			     BigDecimal standD, BigDecimal minumum, BigDecimal maximum, Integer spectralOcount, Integer spectralBcount,
			     Integer spectralAcount, Integer spectralFcount, Integer spectralGcount, Integer spectralKcount, Integer spectralMcount) {
		this.binNum = binNum;
		this.starCount = starCount;
		this.totBMinV = totBMinV.setScale(6, RoundingMode.HALF_UP);
		if (starCount > 0)
			avgBMinV = totBMinV.divide(new BigDecimal(starCount),6,RoundingMode.HALF_UP);
		else
			this.avgBMinV = new BigDecimal("0.0").setScale(6);
		this.mean = mean.setScale(6, RoundingMode.HALF_UP);
		this.median = median.setScale(6, RoundingMode.HALF_UP);
		this.standD = standD.setScale(6, RoundingMode.HALF_UP);
		this.maximum = maximum.setScale(6, RoundingMode.HALF_UP);
		this.minumum = minumum.setScale(6, RoundingMode.HALF_UP);
		
		this.spectralOcount = spectralOcount;
		this.spectralBcount = spectralBcount;
		this.spectralAcount = spectralAcount;
		this.spectralFcount = spectralFcount;
		this.spectralGcount = spectralGcount;
		this.spectralKcount = spectralKcount;
		this.spectralMcount = spectralMcount;
	}
	/**
	 * @return the binNum
	 */
	public Integer getBinNum() {
		return binNum;
	}
	/**
	 * @param binNum the binNum to set
	 */
	public void setBinNum(Integer binNum) {
		this.binNum = binNum;
	}
	/**
	 * @return the starCount
	 */
	public Integer getStarCount() {
		return starCount;
	}
	/**
	 * @param starCount the starCount to set
	 */
	public void setStarCount(Integer starCount) {
		this.starCount = starCount;
	}
	/**
	 * @return the totBMinV
	 */
	public BigDecimal getTotBMinV() {
		return totBMinV;
	}
	/**
	 * @param totBMinV the totBMinV to set
	 */
	public void setTotBMinV(BigDecimal totBMinV) {
		this.totBMinV = totBMinV;
	}
	/**
	 * @return the avgBMinV
	 */
	public BigDecimal getAvgBMinV() {
		return avgBMinV;
	}
	/**
	 * @param avgBMinV the avgBMinV to set
	 */
	public void setAvgBMinV(BigDecimal avgBMinV) {
		this.avgBMinV = avgBMinV;
	}
	
	
	/**
	 * @return the median
	 */
	public BigDecimal getMedian() {
		return median;
	}
	/**
	 * @param median the median to set
	 */
	public void setMedian(BigDecimal median) {
		this.median = median;
	}
	/**
	 * @return the mean
	 */
	public BigDecimal getMean() {
		return mean;
	}
	/**
	 * @param mean the mean to set
	 */
	public void setMean(BigDecimal mean) {
		this.mean = mean;
	}
	/**
	 * @return the standD
	 */
	public BigDecimal getStandD() {
		return standD;
	}
	/**
	 * @param standD the standD to set
	 */
	public void setStandD(BigDecimal standD) {
		this.standD = standD;
	}
	/**
	 * @return the minumum
	 */
	public BigDecimal getMinumum() {
		return minumum;
	}
	/**
	 * @param minumum the minumum to set
	 */
	public void setMinumum(BigDecimal minumum) {
		this.minumum = minumum;
	}
	/**
	 * @return the maximum
	 */
	public BigDecimal getMaximum() {
		return maximum;
	}
	/**
	 * @param maximum the maximum to set
	 */
	public void setMaximum(BigDecimal maximum) {
		this.maximum = maximum;
	}
	
	
	/**
	 * @return the spectralOcount
	 */
	public Integer getSpectralOcount() {
		return spectralOcount;
	}
	/**
	 * @param spectralOcount the spectralOcount to set
	 */
	public void setSpectralOcount(Integer spectralOcount) {
		this.spectralOcount = spectralOcount;
	}
	/**
	 * @return the spectralBcount
	 */
	public Integer getSpectralBcount() {
		return spectralBcount;
	}
	/**
	 * @param spectralBcount the spectralBcount to set
	 */
	public void setSpectralBcount(Integer spectralBcount) {
		this.spectralBcount = spectralBcount;
	}
	/**
	 * @return the spectralAcount
	 */
	public Integer getSpectralAcount() {
		return spectralAcount;
	}
	/**
	 * @param spectralAcount the spectralAcount to set
	 */
	public void setSpectralAcount(Integer spectralAcount) {
		this.spectralAcount = spectralAcount;
	}
	/**
	 * @return the spectralFcount
	 */
	public Integer getSpectralFcount() {
		return spectralFcount;
	}
	/**
	 * @param spectralFcount the spectralFcount to set
	 */
	public void setSpectralFcount(Integer spectralFcount) {
		this.spectralFcount = spectralFcount;
	}
	/**
	 * @return the spectralGcount
	 */
	public Integer getSpectralGcount() {
		return spectralGcount;
	}
	/**
	 * @param spectralGcount the spectralGcount to set
	 */
	public void setSpectralGcount(Integer spectralGcount) {
		this.spectralGcount = spectralGcount;
	}
	/**
	 * @return the spectralKcount
	 */
	public Integer getSpectralKcount() {
		return spectralKcount;
	}
	/**
	 * @param spectralKcount the spectralKcount to set
	 */
	public void setSpectralKcount(Integer spectralKcount) {
		this.spectralKcount = spectralKcount;
	}
	/**
	 * @return the spectralMcount
	 */
	public Integer getSpectralMcount() {
		return spectralMcount;
	}
	/**
	 * @param spectralMcount the spectralMcount to set
	 */
	public void setSpectralMcount(Integer spectralMcount) {
		this.spectralMcount = spectralMcount;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((binNum == null) ? 0 : binNum.hashCode());
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
		RaBin other = (RaBin) obj;
		if (binNum == null) {
			if (other.binNum != null)
				return false;
		} else if (!binNum.equals(other.binNum))
			return false;
		return true;
	}
	
	public int compareTo(RaBin bin) {
		
		if (this == bin)
			return 0;
		
		return this.binNum.compareTo(bin.binNum);
		
	}
	
	
	
	/** This method returns a String display of the person's instance variables.
	 *  @param it receives no input
	 *  @return personData it returns a string of data*/
	public String toFormattedString() {
		return ("Bin Num #: " + getBinNum() + " Star Count: " + getStarCount() + " Tot B-V: " + getTotBMinV() + " " + " Avg B-V: " + getAvgBMinV()
				+" Mean: " + getMean() + " Median: " + getMean() + " StandD: " + getStandD() + " Minimum: " + getMinumum() + " Max: " + getMaximum());
	}
	
	public String toString() {
		
		
		StringBuffer tempString = new StringBuffer(100);
		
		tempString.append(this.getBinNum() + ",");
		tempString.append(this.getStarCount() + ",");
		tempString.append(this.getMean() + ",");
		tempString.append(this.getMedian() + ",");
		tempString.append(this.getStandD() + ",");
		tempString.append(this.getMinumum() + ",");
		tempString.append(this.getMaximum() + ",");
		tempString.append(this.getAvgBMinV() + ",");
		tempString.append(this.getSpectralOcount() + ",");
		tempString.append(this.getSpectralBcount() + ",");
		tempString.append(this.getSpectralAcount() + ",");
		tempString.append(this.getSpectralFcount() + ",");
		tempString.append(this.getSpectralGcount() + ",");
		tempString.append(this.getSpectralKcount() + ",");
		tempString.append(this.getSpectralMcount());

		return tempString.toString();
		
	}
}
