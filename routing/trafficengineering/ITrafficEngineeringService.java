/**
 * 
 */
package com.ibm.floodlight.routing.trafficengineering;

import java.util.List;

import com.ibm.floodlight.opensample.Flow;

import net.floodlightcontroller.core.module.IFloodlightService;

/**
 * @author ${Junho Suh <jhsuh@mmlab.snu.ac.kr>}
 *
 */
public interface ITrafficEngineeringService extends IFloodlightService {
	public void doFlowRescheduling(List<Flow> flowList);
}
