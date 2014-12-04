/**
 * 
 */
package com.ibm.floodlight.opensample;

import java.util.Comparator;
import java.util.PriorityQueue;

import net.decix.jsflow.header.MacHeader;
import net.floodlightcontroller.util.FixedTimer;
import net.floodlightcontroller.util.MACAddress;

/**
 * @author ${Junho Suh <jhsuh@mmlab.snu.ac.kr>}
 *
 */
public class Flow {
//	public class TCPSeqComparator implements Comparator<TCPInfo> {
//		@Override
//		public int compare(TCPInfo info1, TCPInfo info2) {
//			int diff = (int)info2.seqNum - (int)info1.seqNum;
//			if(diff == 0){
//				return 0;
//			}
//			
//			return diff;
//		}
//	}
	
//	private static final int CAPACITY = 1000;
	
	public class TCPInfo{
		long sport;
		long dport;
		long timestamp;
		long seqNum;
		
		public TCPInfo(long sport, long dport, long timestamp, long seqNum){
			this.sport = sport;
			this.dport = dport;
			this.timestamp = timestamp;
			this.seqNum = seqNum;
		}
	}
	
	long firstSeen;
	double duration;
	
	MACPair macPair;
		
	// sample specific
	PriorityQueue<TCPInfo> samples;
	TCPInfo prevInfo;
	
	long prevBytes;
	long prevTime;
	// Flow's packet count and byte count
	long pktCnts;
	long byteCnts;
	
	static final double alpha = .90;
	double Mbps;
	
	long updatedTime;
	boolean scheduled = false;
	
	public Flow(long timestamp, MACPair macPair, MacHeader mh){
		this.firstSeen = timestamp;
		this.duration = 0;
		
		this.macPair = macPair;
		
//		this.samples = new PriorityQueue<TCPInfo>(CAPACITY, new TCPSeqComparator());
		
		TCPInfo tcpInfo = parseTCPHeader(mh);
//		if(tcpInfo != null){
//			this.samples.add(tcpInfo);
//		}
		
		this.prevBytes = tcpInfo.seqNum;
		this.prevTime = tcpInfo.timestamp;
		
		this.Mbps = 0.;
		
		this.updatedTime = timestamp;
	}
	
	public void update(long timestamp, MacHeader mh){
		TCPInfo tcpInfo = parseTCPHeader(mh);
		if(tcpInfo == null){
			return;
		}
		
//		this.samples.add(tcpInfo);
		
		long currentBytes = tcpInfo.seqNum;
		long currentTime = tcpInfo.timestamp;
		
		double bytes = currentBytes - this.prevBytes;
//		double sec = (currentTime - this.prevTime)/1000.;
		double sec = (timestamp - this.updatedTime)/1000.0;
		if(sec > 0){
			double bps = bytes*8.0/sec;
			this.Mbps = alpha*(bps/1000./1000.) + (1-alpha)*this.Mbps;
			
			this.duration += sec;
			
			this.prevBytes = currentBytes;
			this.prevTime = currentTime;
			
			this.updatedTime = timestamp;
		}else{
//			this.Mbps = 0.;
		}
		
//		if(this.samples.size() == 2){
//			TCPInfo info1 = this.samples.poll();
//			TCPInfo info2 = this.samples.poll();
//			
//			double bytes = Math.abs(info1.seqNum - info2.seqNum);
//			double duration = Math.abs(info1.timestamp - info2.timestamp);
//			double sec = duration/1000.;
//			if(sec > 0.){
//				double bps = bytes*8./sec;
//				double Mbps = bps/1000/1000;
//				if(Mbps < 20){
//	                this.Mbps = alpha*Mbps + (1-alpha)*this.Mbps;
//	            }
//			}
//			
//			this.byteCnts += bytes;
//			this.duration = timestamp - firstSeen;
//			this.updatedTime = timestamp;
//			
//			this.samples.add(tcpInfo);
//		}
	}
	
	public boolean getScheduled(){
		return this.scheduled;
	}
	
	public TCPInfo parseTCPHeader(MacHeader mh){
		TCPInfo tcpInfo = null;
		
		if(mh.getType() == 2048){	// IP Header?
			// TCP/IP parsing.
			byte[] ipHeader = mh.getOffCut();
			
			byte[] ttl_proto = new byte[2];
			System.arraycopy(ipHeader, 8, ttl_proto, 0, 2);
			int ttl = ttl_proto[0];
			int proto = ttl_proto[1];
			if(proto == 0x6){				// TCP?
				byte[] sport = new byte[2];
				System.arraycopy(ipHeader, 20, sport, 0, 2);
				int tempa = sport[0] & 0xFF;
				int tempb = sport[1] & 0xFF;
				long tcp_sport = (tempa << 8) + tempb;
				
				byte[] dport = new byte[2];
				System.arraycopy(ipHeader, 22, dport, 0, 2);
				tempa = dport[0] & 0xFF;
				tempb = dport[1] & 0xFF;
				long tcp_dport = (tempa << 8) + tempb;
				
				byte[] seq_num = new byte[4];
				System.arraycopy(ipHeader, 24, seq_num, 0, 4);
				int temp0 = seq_num[0] & 0xFF;
		        int temp1 = seq_num[1] & 0xFF;
				int temp2 = seq_num[2] & 0xFF;
				int temp3 = seq_num[3] & 0xFF;
				long tcp_seq = (((long) temp0 << 24) + (temp1 << 16) + (temp2 << 8) + temp3);
				
				byte[] tcp_timestamp= new byte[4];
				System.arraycopy(ipHeader, 48, tcp_timestamp, 0, 4);
				temp0 = tcp_timestamp[0] & 0xFF;
		        temp1 = tcp_timestamp[1] & 0xFF;
				temp2 = tcp_timestamp[2] & 0xFF;
				temp3 = tcp_timestamp[3] & 0xFF;
				long tcp_ts = (((long) temp0 << 24) + (temp1 << 16) + (temp2 << 8) + temp3);
				
				tcpInfo = new TCPInfo(tcp_sport, tcp_dport, tcp_ts, tcp_seq);
			}
		}
		
		return tcpInfo;
	}
	
	public MACPair getMACPair(){
		return macPair;
	}
	
	public double getMbps(){
		return Mbps;
	}
	
	@Override
    public boolean equals(Object obj){
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        Flow other = (Flow) obj;
        if(!macPair.equals(other.macPair)){
            return false;
        }
        return true;
    }
	
	@Override
	public String toString(){
		String str = "[" + firstSeen
					+ ", " + duration
					+ ", " + macPair.toString()
//					+ ", " + byteCnts + " bytes"
					+ ", " + Mbps + " Mbps]\n";
		
		return str;
	}
}
