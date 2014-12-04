/**
 * 
 */
package com.ibm.floodlight.opensample;

import net.floodlightcontroller.util.MACAddress;

/**
 * @author ${Junho Suh <jhsuh@mmlab.snu.ac.kr>}
 *
 */
public class MACPair {
	MACAddress src;
	MACAddress dst;
	short tcp_sport;
	short tcp_dport;
	
	public MACPair(MACAddress src, MACAddress dst, short tcp_sport, short tcp_dport){
		this.src = src;
		this.dst = dst;
		this.tcp_sport = tcp_sport;
		this.tcp_dport = tcp_dport;
	}
	
	public long getSrc(){
		return src.toLong();
	}
	
	public long getDst(){
		return dst.toLong();
	}
	
	public short getTCPSrc(){
		return tcp_sport;
	}
	
	public short getTCPDst(){
		return tcp_dport;
	}
	
	@Override
    public boolean equals(Object obj){
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        MACPair other = (MACPair) obj;
        if (!src.equals(other.src))
            return false;
        if (!dst.equals(other.dst))
            return false;
        if (tcp_sport != other.tcp_sport)
        	return false;
        if (tcp_dport != other.tcp_dport)
        	return false;
        return true;
    }
	
	@Override
    public int hashCode(){
        final int prime = 2131;
        int result = 1;
//        Long src = new Long(this.src.toLong());
//        Long dst = new Long(this.dst.toLong());
        result = prime * result + src.hashCode();
        result = prime * result + dst.hashCode();
        return result;
    }
	
	@Override
	public String toString(){
		String str = src.toString()
					+ ", " + dst.toString()
					+ ", " + tcp_sport
					+ ", " + tcp_dport;
		
		return str;
	}
}