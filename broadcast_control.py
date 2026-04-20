from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet
import time


class BroadcastControl(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]  #use OpenFlow 1.3 for communication with switch

    BROADCAST_MAC = "ff:ff:ff:ff:ff:ff"  #MAC address used for broadcast packets
    BROADCAST_THRESHOLD = 2   #max allowed broadcasts within time window
    TIME_WINDOW = 10           #time limit (seconds) to count broadcasts

    def __init__(self, *args, **kwargs):
        super(BroadcastControl, self).__init__(*args, **kwargs)
        self.mac_to_port = {}         #maps MAC address to the port it came from (learning switch table)
        self.broadcast_stats = {}     #stores broadcast count and start time per host

    #when switch connects, install default rule
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath    #represents the switch
        ofproto = datapath.ofproto    #OpenFlow protocol used by switch
        parser = datapath.ofproto_parser  #helper to build OpenFlow messages

        #send all unknown packets to controller 
        match = parser.OFPMatch()  #empty match = match all packets
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]  #send packet to controller

        self.add_flow(datapath, 0, match, actions)  #install rule with lowest priority

    #add flow rule to switch
    def add_flow(self, datapath, priority, match, actions, idle_timeout=10):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        #define what actions to apply when match is true
        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]

        #create flow rule message
        mod = parser.OFPFlowMod(datapath=datapath,
                               priority=priority,     #higher priority rules match first
                               match=match,           #conditions (e.g. src, dst)
                               instructions=inst,     #actions to perform
                               idle_timeout=idle_timeout)  #remove rule if unused

        datapath.send_msg(mod)  #send rule to switch

    #handle incoming packets 
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        dpid = datapath.id  #unique ID of switch
        self.mac_to_port.setdefault(dpid, {})  #initialize table for this switch

        #decode packet
        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)  #get ethernet header

        dst = eth.dst  #destination MAC
        src = eth.src  #source MAC
        in_port = msg.match['in_port']  #port where packet came in

        #learn which port this MAC address came from 
        self.mac_to_port[dpid][src] = in_port

        #broadcast handling
        if dst == self.BROADCAST_MAC:
            current_time = time.time()  #get current time

            #first time seeing this host
            if src not in self.broadcast_stats:
                self.broadcast_stats[src] = [1, current_time]  #start count = 1
            else:
                count, start_time = self.broadcast_stats[src]  #get previous count and time

                #if within time window, increase count
                if current_time - start_time <= self.TIME_WINDOW:
                    count += 1
                else:
                    #reset count after time window expires
                    count = 1
                    start_time = current_time

                self.broadcast_stats[src] = [count, start_time]  #update values

            count, _ = self.broadcast_stats[src]  #get updated count

            #if too many broadcasts, drop packet to prevent flooding
            if count > self.BROADCAST_THRESHOLD:
                self.logger.info(f"[BLOCK] Broadcast flood from {src}, count={count}")
                return  #do nothing -> packet is dropped

            #otherwise allow broadcast (normal behavior)
            self.logger.info(f"[ALLOW] Broadcast from {src}, count={count}")
            out_port = ofproto.OFPP_FLOOD  #send to all ports

        else:
            #normal forwarding (learning switch logic)
            if dst in self.mac_to_port[dpid]:
                out_port = self.mac_to_port[dpid][dst]  #send to known port
            else:
                out_port = ofproto.OFPP_FLOOD  #unknown destination → flood

        actions = [parser.OFPActionOutput(out_port)]  #define forwarding action

        #add flow rule for known destination (so future packets skip controller)
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, priority=1, match=match, actions=actions)

        #send packet out immediately
        out = parser.OFPPacketOut(datapath=datapath,
                                 buffer_id=msg.buffer_id,
                                 in_port=in_port,
                                 actions=actions,
                                 data=msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None)

        datapath.send_msg(out)
