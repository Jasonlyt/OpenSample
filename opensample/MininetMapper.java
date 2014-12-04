package com.ibm.floodlight.opensample;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.openflow.protocol.OFPhysicalPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.decix.jsflow.header.CounterRecordHeader;
import net.decix.jsflow.header.CounterSampleHeader;
import net.decix.jsflow.header.FlowSampleHeader;
import net.decix.jsflow.header.GenericInterfaceCounterHeader;
import net.decix.jsflow.header.SampleDataHeader;
import net.decix.jsflow.header.SflowHeader;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IOFSwitchListener;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.topology.NodePortTuple;

public class MininetMapper implements IFloodlightModule, IOFSwitchListener {
	protected static Logger log = LoggerFactory.getLogger(MininetMapper.class);
	
	protected static Map<Long, NodePortTuple> map;
	protected static Map<Long, Long> mapFromSubAgentIDtoDpID;
	protected static IFloodlightProviderService floodlightProviderService;
	
	public static SflowHeader translate(SflowHeader datagram){
		long dpID = -1;
		for(SampleDataHeader sample : datagram.getSampleDataHeaders()){
			if(sample.getSampleDataFormat() == SampleDataHeader.FLOWSAMPLE){
				FlowSampleHeader flowSample = sample.getFlowSampleHeader();
				
				NodePortTuple sourceIDIndex = map.get(flowSample.getSourceIDIndex());
				if(sourceIDIndex == null){
					return null;
				}
				flowSample.setSourceIDIndex(sourceIDIndex.getPortId());
				dpID = sourceIDIndex.getNodeId();
				
				NodePortTuple inputInterface = map.get(flowSample.getInput());
				if(inputInterface == null){
					return null;
				}
				flowSample.setInput(inputInterface.getPortId());
				dpID = inputInterface.getNodeId();
				
				NodePortTuple outputInterface = map.get(flowSample.getOutput());
				if(outputInterface == null){
					return null;
				}
				flowSample.setOutput(outputInterface.getPortId());
				dpID = outputInterface.getNodeId();
				
			}else if(sample.getSampleDataFormat() == SampleDataHeader.COUNTERSAMPLE){
				CounterSampleHeader counterSample = sample.getCounterSampleHeader();
				
				NodePortTuple sourceIDIndex = map.get(counterSample.getSourceIDIndex());
				if(sourceIDIndex == null){
					return null;
				}
				counterSample.setSourceIDIndex(sourceIDIndex.getPortId());
				dpID = sourceIDIndex.getNodeId();
				
				for(CounterRecordHeader record : counterSample.getCounterRecords()){
					if(record.getCounterDataFormat() == CounterRecordHeader.GENERICINTERFACECOUNTER){
						GenericInterfaceCounterHeader counter = record.getGenericInterfaceCounterHeader();
						
						NodePortTuple ifIndex = map.get(counter.getIfIndex());
						if(ifIndex == null){
							return null;
						}
						counter.setIfIndex(ifIndex.getPortId());
						dpID = ifIndex.getNodeId();
					}
				}
			}else{
				// TODO Not implemented yet.
			}
		}
		
		mapFromSubAgentIDtoDpID.put(datagram.getSubAgentID(), dpID);
		return datagram;
	}
	
	public static long getDpIDFromSubAgentID(long subAgnetID){
		return mapFromSubAgentIDtoDpID.get(subAgnetID);
	}
	
	public static IOFSwitch getOFSwitchFromDpID(long dpID){
		return floodlightProviderService.getSwitches().get(dpID);
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		return null;
	}

	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		return null;
	}

	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
        l.add(IFloodlightProviderService.class);
        
        return l;
	}

	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		
		floodlightProviderService = context.getServiceImpl(IFloodlightProviderService.class);
		map = new HashMap<Long, NodePortTuple>();
		mapFromSubAgentIDtoDpID = new HashMap<Long, Long>();
	}

	@Override
	public void startUp(FloodlightModuleContext context) {
		floodlightProviderService.addOFSwitchListener(this);
	}

	@Override
	public void addedSwitch(IOFSwitch sw) {
		for(OFPhysicalPort port : sw.getPorts()){
            String dir = "/sys/devices/virtual/net/" + port.getName() + "/ifindex";
            try {
                char[] buf = new char[10];
                FileReader reader = new FileReader(new File(dir));
                reader.read(buf);
                reader.close();
                long vPortID = new Long(new String(buf).trim());

                synchronized(map){
                	map.put(vPortID, new NodePortTuple(sw.getId(), port.getPortNumber()));
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
	}

	@Override
	public void removedSwitch(IOFSwitch sw) {
		for(OFPhysicalPort port : sw.getPorts()){
			NodePortTuple nodePort = new NodePortTuple(sw.getId(), port.getPortNumber());
			
			synchronized(map){
				for(Long vPort : map.keySet()){
					map.get(vPort).equals(nodePort);
					map.remove(vPort);
				}
			}
		}
	}

	@Override
	public void switchPortChanged(Long switchId) {
		
	}

	@Override
	public String getName() {
		return "mininetmapper";
	}
}
