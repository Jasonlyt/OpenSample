/**
 * 
 */
package com.ibm.floodlight.routing.trafficengineering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.openflow.protocol.OFFlowMod;
import org.openflow.protocol.OFMatch;
import org.openflow.protocol.OFPacketOut;
import org.openflow.protocol.OFType;
import org.openflow.protocol.action.OFAction;
import org.openflow.protocol.action.OFActionOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.floodlight.opensample.Flow;
import com.ibm.floodlight.opensample.MACPair;
import com.ibm.floodlight.routing.LinkAwareRoutingBase;

import net.floodlightcontroller.core.IFloodlightProviderService;
import net.floodlightcontroller.core.IOFSwitch;
import net.floodlightcontroller.core.module.FloodlightModuleContext;
import net.floodlightcontroller.core.module.FloodlightModuleException;
import net.floodlightcontroller.core.module.IFloodlightModule;
import net.floodlightcontroller.core.module.IFloodlightService;
import net.floodlightcontroller.core.util.AppCookie;
import net.floodlightcontroller.devicemanager.SwitchPort;
import net.floodlightcontroller.packet.Ethernet;
import net.floodlightcontroller.routing.Link;
import net.floodlightcontroller.routing.Route;
import net.floodlightcontroller.routing.RouteId;
import net.floodlightcontroller.topology.NodePortTuple;
import net.floodlightcontroller.util.MACAddress;

/**
 * @author ${Junho Suh <jhsuh@mmlab.snu.ac.kr>}
 *
 */
public class TrafficEngineering extends LinkAwareRoutingBase implements IFloodlightModule, ITrafficEngineeringService {
	protected static Logger log = LoggerFactory.getLogger(TrafficEngineering.class);
	
	Set<MACPair> scheduled = new HashSet<MACPair>();
	
	public class WeightedEdge extends DefaultWeightedEdge {
		private static final long serialVersionUID = 1L;
		
		protected Link link;
		
		public WeightedEdge(Link link){
			this.link = link;
		}
		
		public Link getLink(){
			return link;
		}
		
		@Override
		public double getWeight(){
			return super.getWeight();
		}
		
		public String toString(){
			String str = "(src: " + link.getSrc() +" " + link.getSrcPort()
						+ " --> dst: " + link.getDst() + " " + link.getDstPort() + ")"
						+ " " + super.getWeight();
			
			return str;
		}
	}

    protected final short FLOWMOD_DEFAULT_IDLE_TIMEOUT = 60;
    protected final short FLOWMOD_DEFAULT_HARD_TIMEOUT = 0;
    protected final short PRIORITY = 1001;
    protected final int TRAFFIC_ENGINEERING_APP_ID = 42;

    protected final double THRESHOLD = 2.;

    // service dependencies
    protected IFloodlightProviderService floodlightProvider;
    
	/* (non-Javadoc)
	 * @see net.floodlightcontroller.core.module.IFloodlightModule#getModuleServices()
	 */
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleServices() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
        l.add(ITrafficEngineeringService.class);
        
		return l;
	}

	/* (non-Javadoc)
	 * @see net.floodlightcontroller.core.module.IFloodlightModule#getServiceImpls()
	 */
	@Override
	public Map<Class<? extends IFloodlightService>, IFloodlightService> getServiceImpls() {
		Map<Class<? extends IFloodlightService>,
        IFloodlightService> m = new HashMap<Class<? extends IFloodlightService>, IFloodlightService>();
        m.put(ITrafficEngineeringService.class, this);
        
		return m;
	}

	/* (non-Javadoc)
	 * @see net.floodlightcontroller.core.module.IFloodlightModule#getModuleDependencies()
	 */
	@Override
	public Collection<Class<? extends IFloodlightService>> getModuleDependencies() {
		Collection<Class<? extends IFloodlightService>> l = new ArrayList<Class<? extends IFloodlightService>>();
		l.add(IFloodlightProviderService.class);
		
		return l;
	}

	/* (non-Javadoc)
	 * @see net.floodlightcontroller.core.module.IFloodlightModule#init(net.floodlightcontroller.core.module.FloodlightModuleContext)
	 */
	@Override
	public void init(FloodlightModuleContext context)
			throws FloodlightModuleException {
		super.init(context);
		
		floodlightProvider = context.getServiceImpl(IFloodlightProviderService.class);
	}

	/* (non-Javadoc)
	 * @see net.floodlightcontroller.core.module.IFloodlightModule#startUp(net.floodlightcontroller.core.module.FloodlightModuleContext)
	 */
	@Override
	public void startUp(FloodlightModuleContext context)
			throws FloodlightModuleException {
		super.startUp(context);
	}

	/* (non-Javadoc)
	 * @see net.floodlightcontroller.core.IListener#getName()
	 */
	@Override
	public String getName() {
		return "TrafficEngineering";
	}

	/* (non-Javadoc)
	 * @see net.floodlightcontroller.core.IListener#isCallbackOrderingPrereq(java.lang.Object, java.lang.String)
	 */
	@Override
	public boolean isCallbackOrderingPrereq(String type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see net.floodlightcontroller.core.IListener#isCallbackOrderingPostreq(java.lang.Object, java.lang.String)
	 */
	@Override
	public boolean isCallbackOrderingPostreq(String type, String name) {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see com.ibm.floodlight.routing.LinkAwareRoutingBase#recomputeRoutesInvolving(net.floodlightcontroller.routing.Link)
	 */
	@Override
	protected void recomputeRoutesInvolving(Link link) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see com.ibm.floodlight.routing.LinkAwareRoutingBase#recomputeRoutesInvolving(java.lang.Long)
	 */
	@Override
	protected void recomputeRoutesInvolving(Long hostMac) {
		// TODO Auto-generated method stub
		
	}
	
	protected DefaultDirectedWeightedGraph<Long, WeightedEdge> getGraph(){
        DefaultDirectedWeightedGraph<Long, WeightedEdge> g =
                                                new DefaultDirectedWeightedGraph<Long, WeightedEdge>(WeightedEdge.class);

        // add vertex
        synchronized(switchPorts){
                for(Long v : switchPorts.keySet()){
                g.addVertex(v);
            }
        }

        // add edge
        synchronized(switchPortLinks){
                for(Set<Link> links : switchPortLinks.values()){
                    for(Link link : links){
                        long src = link.getSrc();
                        long dst = link.getDst();

                        WeightedEdge l = new WeightedEdge(link);
                        g.addEdge(src, dst, l);
                    }
                }
        }

        return g;
    }
	
	protected Route getLeastCongestedRoute(long src, long dst, DefaultDirectedWeightedGraph<Long, WeightedEdge> g){
        // currently we assume that each host has an only one access point.
        Set<SwitchPort> srcSwSet = hosts.get(src);
        Set<SwitchPort> dstSwSet = hosts.get(dst);

        if(srcSwSet != null && dstSwSet != null){
            SwitchPort srcSw = null;
            for(Iterator<SwitchPort> itr=srcSwSet.iterator(); itr.hasNext();){
                srcSw = itr.next();
            }
            SwitchPort dstSw = null;
            for(Iterator<SwitchPort> itr=dstSwSet.iterator(); itr.hasNext();){
                dstSw = itr.next();
            }

            if(srcSw != null && dstSw != null){
                List<WeightedEdge> routes = DijkstraShortestPath.findPathBetween(g, srcSw.getSwitchDPID(), dstSw.getSwitchDPID());

                if(routes == null){
                    log.debug("Route Not Found");
                    return null;
                }

                List<NodePortTuple> switchPorts = new ArrayList<NodePortTuple>();
                switchPorts.add(new NodePortTuple(srcSw.getSwitchDPID(), srcSw.getPort()));
                for(WeightedEdge l : routes){
                        Link link = l.getLink();
                    NodePortTuple n1 = new NodePortTuple(link.getSrc(), link.getSrcPort());
                    NodePortTuple n2 = new NodePortTuple(link.getDst(), link.getDstPort());
                    switchPorts.add(n1);
                    switchPorts.add(n2);
                }
                switchPorts.add(new NodePortTuple(dstSw.getSwitchDPID(), dstSw.getPort()));
                Route route = new Route(new RouteId(src, dst), switchPorts);

                return route;
            }
        }
        return null;
    }
	
	private void installRoute(long src, long dst, short tcp_sport, short tcp_dport, Route route){
        boolean reqeustFlowRemovedNotifn = false;

        OFFlowMod fm = (OFFlowMod) floodlightProvider.getOFMessageFactory().getMessage(OFType.FLOW_MOD);
        
        OFActionOutput action = new OFActionOutput();
        action.setMaxLength((short)0xffff);
        List<OFAction> actions = new ArrayList<OFAction>();
        actions.add(action);

        OFMatch match = new OFMatch();
        match.setWildcards(OFMatch.OFPFW_ALL
                & (~OFMatch.OFPFW_DL_SRC)
                & (~OFMatch.OFPFW_DL_DST)
                & (~OFMatch.OFPFW_DL_TYPE)
                & (~OFMatch.OFPFW_NW_PROTO)
                & (~OFMatch.OFPFW_TP_SRC)
                & (~OFMatch.OFPFW_TP_DST));
        match.setDataLayerSource(MACAddress.valueOf(src).toBytes());
        match.setDataLayerDestination(MACAddress.valueOf(dst).toBytes());
        match.setDataLayerType((short)2048);
        match.setNetworkProtocol((byte)0x6);
        match.setTransportSource(tcp_sport);
        match.setTransportDestination(tcp_dport);
        fm.setMatch(match).setPriority(PRIORITY);
        fm.setFlags((short)0x0);
        fm.setIdleTimeout(FLOWMOD_DEFAULT_IDLE_TIMEOUT)
                .setHardTimeout(FLOWMOD_DEFAULT_HARD_TIMEOUT)
                .setBufferId(OFPacketOut.BUFFER_ID_NONE)
                .setCookie(AppCookie.makeCookie(TRAFFIC_ENGINEERING_APP_ID, 0))
                .setCommand(OFFlowMod.OFPFC_ADD)
                .setMatch(match)
                .setActions(actions)
                .setLengthU(OFFlowMod.MINIMUM_LENGTH+OFActionOutput.MINIMUM_LENGTH);

        List<NodePortTuple> switchPortList = route.getPath();
        log.debug("PATH:{}", switchPortList);

        for(int index=0; index<switchPortList.size(); index+=2){
//        for (int indx = switchPortList.size()-1; indx > 0; indx-=2) {
            // Assumption: indx and indx+1 will always have the same switch DPID.
            long switchDPID = switchPortList.get(index).getNodeId();
            IOFSwitch sw = floodlightProvider.getSwitches().get(switchDPID);

            if (sw == null) {
                log.debug("******ADD ROUTE::SWITCH NOT FOUND******");
            }
            
         // set buffer id if it is the source switch
            if (index == 0) {
                // Set the flag to request flow-mod removal notifications only for the
                // source switch. The removal message is used to maintain the flow
                // cache. Don't set the flag for ARP messages - TODO generalize check
                if ((reqeustFlowRemovedNotifn) && (match.getDataLayerType() != Ethernet.TYPE_ARP)) {
                    fm.setFlags(OFFlowMod.OFPFF_SEND_FLOW_REM);
                    match.setWildcards(fm.getMatch().getWildcards());
                }
            }
            
            short inPort = switchPortList.get(index).getPortId();
            short outPort = switchPortList.get(index+1).getPortId();
            
            // set input and output ports on the switch
            fm.getMatch().setInputPort(inPort);
            ((OFActionOutput)fm.getActions().get(0)).setPort(outPort);

            // TODO fix me!
//            routePusher.addFlow(fm.toString(), fm, sw.getStringId());
            try {
                log.debug("FLOW_MOD:{}", fm);
                sw.write(fm, null);
                sw.flush();
            } catch (IOException e) {
                log.error("Failure writing flow mod", e);
            }

            try {
                fm = fm.clone();
            } catch (CloneNotSupportedException e) {
                log.error("Failure cloning flow mod", e);
                System.out.println("*****CLONE FAILURE*****");
            }
        }
	}

	/* (non-Javadoc)
	 * @see com.ibm.floodlight.opensample.trafficengineering.ITrafficEngineeringService#doFlowRescheduling(java.util.List)
	 */
	@Override
	public void doFlowRescheduling(List<Flow> flowList) {
		DefaultDirectedWeightedGraph<Long, WeightedEdge> g = getGraph();

        for(Flow flow : flowList){
        	MACPair macPair = flow.getMACPair();
            long src = macPair.getSrc();
            long dst = macPair.getDst();
            short tcp_sport = macPair.getTCPSrc();
            short tcp_dport = macPair.getTCPDst();

            Route route = getLeastCongestedRoute(src, dst, g);

            if(route != null){
//                log.debug("ROUTE:{}", route);
            	if(!flow.getScheduled()){
            		installRoute(src, dst, tcp_sport, tcp_dport, route);
            	}

                // update graph
                List<NodePortTuple> path = route.getPath();
                path.remove(0);
                path.remove(path.size()-1);

                if(path.size() != 0){
                        Iterator<NodePortTuple> itr = path.iterator();
                    while(true){
                        NodePortTuple n1 = itr.next();
                        NodePortTuple n2 = itr.next();

                        WeightedEdge link = g.getEdge(n1.getNodeId(), n2.getNodeId());
                        double bw = flow.getMbps();
                        g.setEdgeWeight(link, bw);

                        if(!itr.hasNext()){
                            break;
                        }
                    }
                }
            }
        }
	}
}
