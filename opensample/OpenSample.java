package com.ibm.floodlight.opensample;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.FixedReceiveBufferSizePredictorFactory;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.jboss.netty.handler.codec.frame.FrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.floodlight.routing.trafficengineering.ITrafficEngineeringService;

import net.decix.jsflow.header.CounterSampleHeader;
import net.decix.jsflow.header.FlowRecordHeader;
import net.decix.jsflow.header.FlowSampleHeader;
import net.decix.jsflow.header.MacHeader;
import net.decix.jsflow.header.RawPacketHeader;
import net.decix.jsflow.header.SampleDataHeader;
import net.decix.jsflow.header.SflowHeader;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.threadpool.IThreadPoolService;
import net.floodlightcontroller.util.FixedTimer;
import net.floodlightcontroller.util.MACAddress;

public class OpenSample implements IFloodlightModule {
	protected static Logger log = LoggerFactory.getLogger(OpenSample.class);
	
	protected IFloodlightProviderService floodlightProviderService;
	protected IThreadPoolService threadPool;
	
	// sFlow collector variables
	sFlowCollector collector;
	sFlowDecoder parser;
	protected static final int SFLOW_DEFAULT_PORT = 6343;
	protected static final int BUFFSIZE = 65536;
	
	protected Map<IOFSwitch, ConcurrentHashMap<MACPair, Flow>> flowTables;
	
	protected ITrafficEngineeringService trafficEngineeringService;
	protected Thread teThread;
	protected FixedTimer teTimer;
	protected static final double ELEPHANT_THREASHOLD = 1.0;
	protected static final long FLOWTIMEOUT = 100L;

	protected Thread te;
	protected BlockingQueue<Flow> needToTE;
	protected static final int CAPACITY = 1000;
	protected static final long INTERVAL = 100L;
	
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
        
		return l;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		Map<Class<? extends IFloodlightService>,
        IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
        
		return m;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
        l.add(IFloodlightProviderService.class);
        l.add(IThreadPoolService.class);
        l.add(ITrafficEngineeringService.class);
        
        return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		floodlightProviderService = context.getServiceImpl(IFloodlightProviderService.class);
		threadPool = context.getServiceImpl(IThreadPoolService.class);
		
		flowTables = new HashMap<IOFSwitch, ConcurrentHashMap<MACPair, Flow>>();
		
		trafficEngineeringService = context.getServiceImpl(ITrafficEngineeringService.class);
		// TODO initialize sampling parameters
		needToTE = new ArrayBlockingQueue<Flow>(CAPACITY);
		
		teThread = new Thread(){
			@Override
			public void run(){
				while(true){
					List<SflowHeader> datagrams = new ArrayList<SflowHeader>();
					int numSamples = collector.samples.drainTo(datagrams);
					if(numSamples == 0){
						continue;
					}
					log.debug("Num Samples: {}", numSamples);
					
					for(SflowHeader datagram : datagrams){
						long dpID = datagram.getSubAgentID();
						IOFSwitch sw = getSwitchFromDpID(dpID);
						long timestamp = datagram.getSysUptime();
						
						for(SampleDataHeader sdh : datagram.getSampleDataHeaders()){
		                	if(sdh.getSampleDataFormat() == SampleDataHeader.FLOWSAMPLE){
		                		FlowSampleHeader flowSample = sdh.getFlowSampleHeader();
		                		processFlowSample(sw, timestamp, flowSample);
		                	}else if(sdh.getSampleDataFormat() == SampleDataHeader.COUNTERSAMPLE){
		                		CounterSampleHeader csh = sdh.getCounterSampleHeader();
		                	}else{
		                		// not supported protocol.
		                	}
		                }
//						log.debug("TABLE:{}", flowTables.get(sw).values());
					}
					
					// remove flow tables
					for(Map<MACPair, Flow> table : flowTables.values()){
						for(Iterator<MACPair> itr = table.keySet().iterator(); itr.hasNext();){
							MACPair macPair = (MACPair) itr.next();
							Flow flow = table.get(macPair);
							if(System.currentTimeMillis() - flow.updatedTime >= FLOWTIMEOUT){
								log.debug("FLOWTIMEOUT");
								flow.Mbps = 0.;
								flow.scheduled = false;
							}
						}
					}
					
					
					Map<MACPair, Flow> flowSet = new HashMap<MACPair, Flow>();
					synchronized(flowTables){
						for(IOFSwitch s : flowTables.keySet()){
							Map<MACPair, Flow> table = flowTables.get(s);
							for(Flow flow : table.values()){
								if(flow.Mbps >= ELEPHANT_THREASHOLD){
									if(!flowSet.containsKey(flow.macPair)){
										flowSet.put(flow.macPair, flow);
									}else{
										Flow e = flowSet.get(flow.macPair);
										if(e.Mbps < flow.Mbps){
											flowSet.put(flow.macPair, flow);
										}
									}
								}
							}
						}
					}
//					if(needToTE.size() > 0){
//						needToTE.clear();
//					}
//					needToTE.addAll(flowSet.values());
					
					List<Flow> flowList = new ArrayList<Flow>(flowSet.values());
	                if(flowList.size() > 0){
	                	log.debug("NUM:{} FLOWLIST:{}", flowList.size(), flowList);
	            		trafficEngineeringService.doFlowRescheduling(flowList);
		                long end = System.currentTimeMillis();
		                
		                for(Flow flow : flowList){
		                	MACPair macPair = flow.macPair;
		                	for(IOFSwitch sw : flowTables.keySet()){
		                		Map<MACPair, Flow> table = flowTables.get(sw);
		                		Flow f = table.get(macPair);
		                		if(f != null){
		                			f.scheduled = true;
		                		}
		                	}
		                }
	                }
				}
			}
		};
		
//		xxx = new FixedTimer(0L, INTERVAL){
//			@Override
//			public void run(){
//				List<SflowHeader> datagrams = new ArrayList<SflowHeader>();
//				int numSamples = collector.portSamples.drainTo(datagrams);
//				
//				for(SflowHeader datagram : datagrams){
//					long dpID = datagram.getSubAgentID();
//					IOFSwitch sw = getSwitchFromDpID(dpID);
//					long timestamp = datagram.getSysUptime();
//				}
//			}
//		};
//		te = new Thread(){
//			@Override
//			public void run(){
//				while(true){
//					List<Flow> flowList = new ArrayList<Flow>();
//					
//					needToTE.drainTo(flowList);
//	                if(flowList.size() > 0){
//	                	log.debug("NUM:{} FLOWLIST:{}", flowList.size(), flowList);
//	            		trafficEngineeringService.doFlowRescheduling(flowList);
//		                long end = System.currentTimeMillis();
//		                
//		                for(Flow flow : flowList){
//		                	MACPair macPair = flow.macPair;
//		                	for(IOFSwitch sw : flowTables.keySet()){
//		                		Map<MACPair, Flow> table = flowTables.get(sw);
//		                		Flow f = table.get(macPair);
//		                		if(f != null){
//		                			f.scheduled = true;
//		                		}
//		                	}
//		                }
//	                }
//				}
//			}	
//		};
		// initialize sFlow collector
		collector = new sFlowCollector();
		parser = new sFlowDecoder();
		ChannelFactory factory = new NioDatagramChannelFactory(threadPool.getScheduledExecutor());
        ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(factory);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory(){
            public ChannelPipeline getPipeline(){
                ChannelPipeline lChannelPipeline = Channels.pipeline();
                lChannelPipeline.addLast("decoder", parser);
                lChannelPipeline.addLast("handler", collector);

                return lChannelPipeline;
            }
        });

        bootstrap.setOption("receiveBufferSize", BUFFSIZE);
        FixedReceiveBufferSizePredictorFactory bufferSizePredictor = new FixedReceiveBufferSizePredictorFactory(BUFFSIZE);
        bootstrap.setOption("receiveBufferSizePredictorFactory", bufferSizePredictor);
        bootstrap.bind(new InetSocketAddress(SFLOW_DEFAULT_PORT));
	}

	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		teThread.start();
//		te.start();
		
//		teTimer = new FixedTimer(0L, INTERVAL){
//			@Override
//			public void run(){
//				synchronized(flowTables){
//					long start = System.currentTimeMillis();
//					Map<MACPair, Flow> flowSet = new HashMap<MACPair, Flow>();
//					for(IOFSwitch s : flowTables.keySet()){
//						Map<MACPair, Flow> table = flowTables.get(s);
//						for(Flow flow : table.values()){
//							if( ELEPHANT_THREASHOLD <= flow.Mbps){
//								if(!flowSet.containsKey(flow.macPair)){
//									flowSet.put(flow.macPair, flow);
//								}else{
//									Flow e = flowSet.get(flow.macPair);
//									if(e.Mbps > flow.Mbps){
//										flowSet.put(flow.macPair, flow);
//									}
//								}
//							}
//						}
//					}
//					// re-scheduling
//					List<Flow> flowList = new ArrayList<Flow>(flowSet.values());
//	                if(flowList.size() > 0){
//	                	log.debug("NUM:{} FLOWLIST:{}", flowList.size(), flowList);
//	            		trafficEngineeringService.doFlowRescheduling(flowList);
//		                long end = System.currentTimeMillis();
//		                log.debug("TIMETOSCHEDULE:{}", end-start);
//		                
//		                for(Flow flow : flowList){
//		                	MACPair macPair = flow.macPair;
//		                	for(IOFSwitch sw : flowTables.keySet()){
//		                		Map<MACPair, Flow> table = flowTables.get(sw);
//		                		Flow f = table.get(macPair);
//		                		if(f != null){
//		                			f.scheduled = true;
//		                		}
//		                	}
//		                }
//	                }
//				}
//			}
//		};
	}
	
	public IOFSwitch getSwitchFromDpID(long dpID){
		return floodlightProviderService.getSwitches().get(dpID);
	}
	
	public void processFlowSample(IOFSwitch sw, long timestamp, FlowSampleHeader flowSample){
		if(flowSample.getSourceIDType() == 0){
			long portID = flowSample.getSourceIDIndex();
			long samplingRate = flowSample.getSamplingRate();
			long samplePool = flowSample.getSamplePool();
			long input = flowSample.getInput();
			long output = flowSample.getOutput();
			
			for(FlowRecordHeader frh : flowSample.getFlowRecords()){
				if(frh.getFlowDataFormat() == FlowRecordHeader.RAWPACKETHEADER){
					RawPacketHeader rph = frh.getRawPacketHeader();
					if(rph.getHeaderProtocol() == RawPacketHeader.ETHERNET_ISO88023){
						long frameLen = rph.getFrameLength();
						MacHeader macHeader = rph.getMacHeader();
						
						MACAddress src = MACAddress.valueOf(macHeader.getSource());
						MACAddress dst = MACAddress.valueOf(macHeader.getDestination());
						// TCP
						long tcp_sport = parsingTCPSrcPort(macHeader);
						long tcp_dport = parsingTCPDstPort(macHeader);
						MACPair macPair = new MACPair(src, dst, (short)tcp_sport, (short)tcp_dport);
						
						// add flow table
						synchronized(flowTables){
							if(!flowTables.containsKey(sw)){
								flowTables.put(sw, new ConcurrentHashMap<MACPair, Flow>());
							}
							Map<MACPair, Flow> table = flowTables.get(sw);
							
							if(table.containsKey(macPair)){
								Flow flow = table.get(macPair);
								flow.update(timestamp, macHeader);
							}else{
								Flow flow = new Flow(timestamp, macPair, macHeader);
								table.put(macPair, flow);
							}
						}
					}
				}
			}
		}
	}
	
	public long parsingTCPSrcPort(MacHeader mh){
		long tcp_sport = -1;
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
				tcp_sport = (tempa << 8) + tempb;
			}
		}
		
		return tcp_sport;
	}
	
	public long parsingTCPDstPort(MacHeader mh){
		long tcp_dport = -1;
		if(mh.getType() == 2048){	// IP Header?
			// TCP/IP parsing.
			byte[] ipHeader = mh.getOffCut();
			
			byte[] ttl_proto = new byte[2];
			System.arraycopy(ipHeader, 8, ttl_proto, 0, 2);
			int ttl = ttl_proto[0];
			int proto = ttl_proto[1];
			if(proto == 0x6){				// TCP?
				byte[] dport = new byte[2];
				System.arraycopy(ipHeader, 22, dport, 0, 2);
				int tempa = dport[0] & 0xFF;
				int tempb = dport[1] & 0xFF;
				tcp_dport = (tempa << 8) + tempb;
			}
		}
		
		return tcp_dport;
	}
	
	//
	// sFlow collector class
	//
	
	public class sFlowCollector extends SimpleChannelUpstreamHandler {
		protected Logger log = LoggerFactory.getLogger(sFlowCollector.class);
		
		protected BlockingQueue<SflowHeader> samples;
		protected BlockingQueue<SflowHeader> portSamples;
		protected static final int BUFFSIZE = 65536;
		
		public sFlowCollector(){
			samples = new ArrayBlockingQueue<SflowHeader>(BUFFSIZE);
			portSamples = new ArrayBlockingQueue<SflowHeader>(BUFFSIZE);
		}
		
		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
			if(e.getMessage() instanceof SflowHeader){
                SflowHeader datagram = (SflowHeader) e.getMessage();
                datagram = MininetMapper.translate(datagram);
                if(datagram == null){
                	// switch is not registered yet.
                	return;
                }
                long dpID = MininetMapper.getDpIDFromSubAgentID(datagram.getSubAgentID());
//                IOFSwitch sw = MininetMapper.getOFSwitchFromDpID(dpID);
                datagram.setSubAgentID(dpID);
                datagram.setSystemUptime(System.currentTimeMillis());
                
                samples.add(datagram);
                portSamples.add(datagram);
			}
		}
	}
	
	public class sFlowDecoder extends FrameDecoder {
        protected Logger log = LoggerFactory.getLogger(sFlowDecoder.class);
        
        @Override
        protected Object decode(ChannelHandlerContext context, Channel channel, ChannelBuffer buffer) throws Exception {
            byte[] data = new byte[buffer.readableBytes()];
            buffer.readBytes(data);
            SflowHeader datagram = SflowHeader.parse(data);
            return datagram;
        }
	}
}
