/**
 * 
 */
package com.ibm.floodlight.opensample;

import net.floodlightcontroller.util.MACAddress;

/**
 * @author ${Junho Suh <jhsuh@mmlab.snu.ac.kr>}
 *
 */
public final class FlowStatistics {
	MACAddress src;
	MACAddress dst;
	
	long input;
	long output;
	
	double expectedPktCnt;
	double varPktCnt;
	double expectedByteCnt;
	double varByteCnt;
	
	public FlowStatistics(MACAddress src, MACAddress dst, long input, long output, double expectedPktCnt, double varPktCnt, double expectedByteCnt, double varByteCnt){
		this.src = src;
		this.dst = dst;
		this.input = input;
		this.output = output;
		this.expectedPktCnt = expectedPktCnt;
		this.varPktCnt = varPktCnt;
		this.expectedByteCnt = expectedByteCnt;
		this.varByteCnt = varByteCnt;
	}
	
	public static FlowStatistics doEstimate(MACAddress src, MACAddress dst, long input, long output, long pktCnt, long byteCnt, long totalSamples, long totalPkts){
		final double mleSample = pktCnt / totalSamples;
		final double avgBytes = (double)byteCnt / (double)pktCnt;
		
		// simple scaling
		double expectedPktsCnt = mleSample * totalPkts;
		double varPkts = 196 * Math.sqrt(1/(double)pktCnt);
		
		double expectedBytesCnt = avgBytes * expectedPktsCnt;
		double varBytes = 0.;
		
		return new FlowStatistics(src, dst, input, output, expectedPktsCnt, varPkts, expectedBytesCnt, varBytes);
		
	}
	
	@Override
	public String toString(){
		String str = "[ " + src.toString()
					+ " " + dst.toString()
					+ " " + expectedPktCnt + " " + varPktCnt
					+ " " + expectedByteCnt + " " + varByteCnt + " ]";
		
		return str;
	}
}
