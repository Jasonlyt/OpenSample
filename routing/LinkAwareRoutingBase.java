package com.ibm.floodlight.routing;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.devicemanager.IDevice;
import net.floodlightcontroller.devicemanager.IDeviceListener;
import net.floodlightcontroller.devicemanager.IDeviceService;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryListener;
import net.floodlightcontroller.linkdiscovery.ILinkDiscoveryService;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.topology.ITopologyListener;
import net.floodlightcontroller.topology.ITopologyService;

/**
 * This is an abstract class that listens to the {@link ILinkDiscoveryService}
 * to provide an up-to-date view of the topology of the network via switchPorts
 * and switchPortLinks. Note that all accesses to them must be synchronized on
 * them and if acquiring both locks, switchPorts must be acquired first.
 * Further, modules deriving from this one must call super.init() and
 * super.startUp() to properly initialize this functionality.
 * 
 * @author Colin Dixon (IBM)
 */
public abstract class LinkAwareRoutingBase implements
    ILinkDiscoveryListener, IFloodlightModule, IDeviceListener,
    ITopologyListener {

    protected Logger log = LoggerFactory.getLogger(LinkAwareRoutingBase.class);
    
    //TODO: should be protected, instead public to allow PDST test to get access
    public Map<Long, Set<Short>> switchPorts; // Set of ports for each switch
    public Map<SwitchPort, Set<Link>> switchPortLinks; // Set of links organized by node port tuple
    public Map<Long, Set<SwitchPort>> hosts; //list of attachment points for each host
    
    protected ILinkDiscoveryService linkDiscovery;
    protected IDeviceService devSvc;
    protected ITopologyService topoSvc;

    /**
     * <p>
     * The provided methods call this function whenever a link is added, removed
     * or updated to inform the routing provider that routes involving this link
     * should be recomputed.
     * </p>
     * <p>
     * The various cases should be determined by looking at the switchPorts and
     * switchPortLinks data structures.
     * </p>
     * 
     * @param link
     *            The link that has been added, removed or updated
     */
    abstract protected void recomputeRoutesInvolving(Link link);
    
    /**
     * <p>
     * The provided methods call this function whenever a host is added, removed
     * or moves in the network such that the routing provider can modify any
     * relevant routes.
     * </p>
     * <p>
     * The various cases should be determined by looking at the hosts data
     * structure.
     * </p>
     * 
     * @param hostMac
     *            The MAC address of the host that has been added, removed or
     *            moved
     */
    abstract protected void recomputeRoutesInvolving(Long hostMac);
    
    @Override
    public void init(FloodlightModuleContext context) throws FloodlightModuleException {
        linkDiscovery = context.getServiceImpl(ILinkDiscoveryService.class);
        devSvc = context.getServiceImpl(IDeviceService.class);
        topoSvc = context.getServiceImpl(ITopologyService.class);

        switchPortLinks = new HashMap<SwitchPort, Set<Link>>();
        switchPorts = new HashMap<Long, Set<Short>>();
        hosts = new HashMap<Long, Set<SwitchPort>>();
    }
    
    @Override
    public void startUp(FloodlightModuleContext context) throws FloodlightModuleException {
        linkDiscovery.addListener(this);
        devSvc.addListener(this);
        topoSvc.addListener(this);
        
        log.debug("LinkAwareRoutingBase started up!");
    }
    
    @Override
    public void linkDiscoveryUpdate(LDUpdate update) {
        switch (update.getOperation()) {
            case LINK_UPDATED:
            case PORT_UP:
//                boolean added = (((update.getSrcPortState() & OFPortState.OFPPS_STP_MASK.getValue()) != OFPortState.OFPPS_STP_BLOCK.getValue()) && ((update.getDstPortState() & OFPortState.OFPPS_STP_MASK.getValue()) != OFPortState.OFPPS_STP_BLOCK.getValue()));
//                if (added) {
                    addOrUpdateLink(update.getSrc(), update.getSrcPort(),
                                    update.getDst(), update.getDstPort(),
                                    update.getType());
//                } else {
//                    removeLink(update.getSrc(), update.getSrcPort(),
//                               update.getDst(), update.getDstPort());
//                }
                break;
            case LINK_REMOVED:
            case PORT_DOWN:
                removeLink(update.getSrc(), update.getSrcPort(),
                           update.getDst(), update.getDstPort());
                break;
            case SWITCH_UPDATED:
                updatedSwitch(update.getSrc());
                break;
            default:
                // TODO: handle SWITCH_REMOVED
                log.error("Unsupported LDUpdate Operation: "
                          + update.getOperation());
                break;
        }
    }

    @Override
    public void linkDiscoveryUpdate(List<LDUpdate> updateList) {
        for (LDUpdate u : updateList) {
            linkDiscoveryUpdate(u);
        }
    }

    protected void addOrUpdateLink(long srcSw, short srcPort, long dstSw,
                                   short dstPort, LinkType type) {
        SwitchPort n1 = new SwitchPort(srcSw, srcPort);
        SwitchPort n2 = new SwitchPort(dstSw, dstPort);
        Link l = new Link(srcSw, srcPort, dstSw, dstPort);

        synchronized (switchPorts) {
            if (srcSw != 0) {
                if (switchPorts.get(srcSw) == null) {
                    switchPorts.put(srcSw, new HashSet<Short>());
                }
                switchPorts.get(srcSw).add(srcPort);
            }

            if (dstSw != 0) {
                if (switchPorts.get(dstSw) == null) {
                    switchPorts.put(dstSw, new HashSet<Short>());
                }
                switchPorts.get(dstSw).add(dstPort);
            }
        }

        if (srcSw != 0 && dstSw != 0) {
            synchronized (switchPortLinks) {
                Set<Link> s = switchPortLinks.get(n1);
                if (s == null) {
                    switchPortLinks.put(n1, new HashSet<Link>());
                    s = switchPortLinks.get(n1);
                }
                s.add(l);

                s = switchPortLinks.get(n2);
                if (s == null) {
                    switchPortLinks.put(n2, new HashSet<Link>());
                    s = switchPortLinks.get(n2);
                }
                s.add(l);
            }
        }
        
        recomputeRoutesInvolving(new Link(srcSw, srcPort, dstSw, dstPort));
    }

    protected void removeLink(long srcSw, short srcPort, long dstSw,
                              short dstPort) {
        SwitchPort n1 = new SwitchPort(srcSw, srcPort);
        SwitchPort n2 = new SwitchPort(dstSw, dstPort);
        Link l = new Link(srcSw, srcPort, dstSw, dstPort);

        if (srcSw != 0 && dstSw != 0) {
            synchronized (switchPorts) {
                synchronized (switchPortLinks) {
                    Set<Link> s = switchPortLinks.get(n1);
                    if (s != null) {
                        s.remove(l);
                        if (s.isEmpty()) {
                            switchPortLinks.remove(n1);
                            switchPorts.get(n1.getSwitchDPID()).remove(n1.getPort());
                        }
                    }

                    s = switchPortLinks.get(n2);
                    if (s != null) {
                        s.remove(l);
                        if (s.isEmpty()) {
                            switchPortLinks.remove(n2);
                            switchPorts.get(n2.getSwitchDPID()).remove(n2.getPort());
                        }
                    }
                }
            }
        }
        
        recomputeRoutesInvolving(new Link(srcSw, srcPort, dstSw, dstPort));
    }

    protected void updatedSwitch(long sw) {
        // left intentionally blank for now, we only care about links
    }

    @Override
    public void deviceAdded(IDevice device) {
        Long nodeId = device.getMACAddress();
        Set<SwitchPort> s = new HashSet<SwitchPort>();
        for (SwitchPort p : device.getAttachmentPoints()) {
            long sw = p.getSwitchDPID();
            short port = (short)p.getPort(); //TODO: make this a long
            s.add(new SwitchPort(sw, port));
        }
        boolean recompute = false;

        synchronized (hosts) {
            if (!hosts.containsKey(nodeId) || !hosts.get(nodeId).equals(s)) {
                // new host or changed attachment points
                hosts.put(nodeId, s);
                recompute = true;
            }
        }

        if (recompute) {
            recomputeRoutesInvolving(device.getMACAddress());
        }
    }

    @Override
    public void deviceRemoved(IDevice device) {
        Long nodeId = device.getMACAddress();
        synchronized (hosts) {
            if (hosts.containsKey(nodeId)) {
                hosts.remove(nodeId);
            }
        }

        recomputeRoutesInvolving(device.getMACAddress());
    }

    @Override
    public void deviceMoved(IDevice device) {
        deviceAdded(device);
    }

    @Override
    public void deviceIPV4AddrChanged(IDevice device) {
        // intentionally ignored
    }

    @Override
    public void deviceVlanChanged(IDevice device) {
        // ignoring for now
    }
    
    @Override
    public void topologyChanged() {
        /*
         * topologyChanged really just means that switch clusters (openflow
         * islands) were recomputed which means that attachment points may have
         * changed without a deviceMoved event. So, we call deviceAdded to
         * update all attachment points.
         */

        // get the IDevice objects for all known devices
        ArrayList<IDevice> devices = new ArrayList<IDevice>();
        synchronized(hosts){
            for(long h:hosts.keySet()){
                IDevice d = devSvc.findDevice(h, null, null, null, null);
                if(d!=null){
                    devices.add(d);
                }
            }
        }
        
        // re-add them resulting in calling and processing getAttachmentPoints
        for(IDevice dev:devices){
           deviceAdded(dev);
        }
    }

    
    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleServices() {
        Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
        return l;
    }

    @Override
    public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
        Map<Class<? extends IFloodlightService>, IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
        return m;
    }

    @Override
    public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
        Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
        l.add(ILinkDiscoveryService.class);
        l.add(IDeviceService.class);
        l.add(ITopologyService.class);
        return l;
    }
}
