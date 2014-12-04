package com.ibm.floodlight.arpinterposer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.openflow.protocol.OFMessage;
import org.openflow.protocol.OFType;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFPacketIn;
import org.openflow.protocol.OFPacketOut;
import org.openflow.protocol.OFPort;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.floodlightcontroller.core.FloodlightContext;
import net.floodlightcontroller.core.IOFMessageListener;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.packet.ARP;
import net.floodlightcontroller.packet.IPv4;

public class ArpInterposer implements IOFMessageListener, IFloodlightModule {
    protected static Logger log = LoggerFactory.getLogger(ArpInterposer.class);
    protected IFloodlightProviderService floodlightProvider;
    protected IDeviceService deviceManager;

    @Override
    public String getName() {
        return "arpinterposer";
    }

	@Override
	public boolean isCallbackOrderingPrereq(OFType type, String name) {
	    // we need to run after the device manager
	    return type.equals(OFType.PACKET_IN) && name.equals("devicemanager");
	}

	@Override
	public boolean isCallbackOrderingPostreq(OFType type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Command receive(IOFSwitch sw, OFMessage msg, FloodlightContext cntx) {
        OFPacketIn pi = (OFPacketIn) msg;
        OFMatch match = new OFMatch();
        match.loadFromPacket(pi.getPacketData(), pi.getInPort());
        Long dlAddr = Ethernet.toLong(match.getDataLayerSource());
        
        Ethernet ethReq = new Ethernet();
        ethReq.deserialize(pi.getPacketData(), 0, pi.getPacketData().length);
        if (ethReq.getPayload() instanceof ARP) {
            ARP arpReq = (ARP) ethReq.getPayload();
            if ((arpReq.getProtocolType() == ARP.PROTO_TYPE_IP)
                    && (Ethernet.toLong(arpReq.getSenderHardwareAddress()) == dlAddr)
                    && (arpReq.getOpCode() == ARP.OP_REQUEST)) {
                Integer dstIp = IPv4.toIPv4Address(arpReq.getTargetProtocolAddress());
                Iterator<? extends IDevice> devs = deviceManager.queryDevices(null, null, dstIp, null, null);
                IDevice dst = devs.hasNext() ? devs.next() : null;
                if (dst != null) {
                    // Build ARP and Eth packets
                    ARP arpRpl = new ARP()
                        .setHardwareType(ARP.HW_TYPE_ETHERNET)
                        .setProtocolType(ARP.PROTO_TYPE_IP)
                        .setHardwareAddressLength((byte) 6)
                        .setProtocolAddressLength((byte) 4)
                        .setOpCode(ARP.OP_REPLY)
                        .setSenderHardwareAddress(Ethernet.toByteArray(dst.getMACAddress()))
                        .setSenderProtocolAddress(arpReq.getTargetProtocolAddress())
                        .setTargetHardwareAddress(arpReq.getSenderHardwareAddress())
                        .setTargetProtocolAddress(arpReq.getSenderHardwareAddress());
                    Ethernet ethRpl = new Ethernet()
                        .setSourceMACAddress(Ethernet.toByteArray(dst.getMACAddress()))
                        .setDestinationMACAddress(arpReq.getSenderHardwareAddress())
                        .setEtherType(Ethernet.TYPE_ARP);
                    ethRpl.setPayload(arpRpl);
                    
                    // serialize and wrap in a packet out
                    byte[] data = ethRpl.serialize();
                    OFPacketOut po = new OFPacketOut();
                    po.setBufferId(OFPacketOut.BUFFER_ID_NONE);
                    po.setInPort(OFPort.OFPP_NONE);
    
                    // set actions
                    List<OFAction> actions = new ArrayList<OFAction>();
                    actions.add(new OFActionOutput(pi.getInPort(), (short) 0));
                    po.setActions(actions);
                    po.setActionsLength((short) OFActionOutput.MINIMUM_LENGTH);
    
                    // set data
                    po.setLengthU(OFPacketOut.MINIMUM_LENGTH + po.getActionsLength() + data.length);
                    po.setPacketData(data);
                    
                    log.debug("Sending ARP Reply {}", arpRpl);
    
                    // send
                    try {
                        sw.write(po, cntx);
                        return Command.STOP;
                    } catch (IOException e) {
                        log.error("Failure sending ARP Reply {}", arpRpl);
                    }
                } else {
                    log.debug("Unknown dest IP {}, flooding", IPv4.fromIPv4Address(dstIp));
                }
            }
        }
        
        return Command.CONTINUE;
	}

    @Override
    public Collection<Class<? extends IFloodlightService>>
            getModuleServices() {
        return new ArrayList<Class<? extends IFloodlightService>>();
    }

    @Override
    public Map<Class<? extends IFloodlightService>, IFloodlightService>
            getServiceImpls() {
        return new HashMap<Class <? extends IFloodlightService>, IFloodlightService>();
    }

    @Override
    public Collection<Class<? extends IFloodlightService>>
            getModuleDependencies() {
        Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
        l.add(IDeviceService.class);
        l.add(IFloodlightProviderService.class);
        return l;
    }

    @Override
    public void init(FloodlightModuleContext context) throws FloodlightModuleException {
        this.floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
        this.deviceManager = context.getServiceImpl(IDeviceService.class);
    }

    @Override
    public void startUp(FloodlightModuleContext context) {
        floodlightProvider.addOFMessageListener(OFType.PACKET_IN, this); 
    }
}
