from ryu.base import app_manager
from ryu.controller import ofp_event
from ryu.controller.handler import CONFIG_DISPATCHER, MAIN_DISPATCHER
from ryu.controller.handler import set_ev_cls
from ryu.ofproto import ofproto_v1_3
from ryu.lib.packet import packet, ethernet
import time


class BroadcastControl(app_manager.RyuApp):
    OFP_VERSIONS = [ofproto_v1_3.OFP_VERSION]

    BROADCAST_MAC = "ff:ff:ff:ff:ff:ff"
    BROADCAST_THRESHOLD = 2   #max allowed broadcasts
    TIME_WINDOW = 10           #time limit (seconds)

    def __init__(self, *args, **kwargs):
        super(BroadcastControl, self).__init__(*args, **kwargs)
        self.mac_to_port = {}         #maps MAC address to the port it came from
        self.broadcast_stats = {}     #stores broadcast count per host

    #when switch connects, install default rule
    @set_ev_cls(ofp_event.EventOFPSwitchFeatures, CONFIG_DISPATCHER)
    def switch_features_handler(self, ev):
        datapath = ev.msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        #send unknown packets to controller
        match = parser.OFPMatch()
        actions = [parser.OFPActionOutput(ofproto.OFPP_CONTROLLER,
                                          ofproto.OFPCML_NO_BUFFER)]

        self.add_flow(datapath, 0, match, actions)

    #add flow rule to switch
    def add_flow(self, datapath, priority, match, actions, idle_timeout=10):
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        inst = [parser.OFPInstructionActions(ofproto.OFPIT_APPLY_ACTIONS,
                                             actions)]

        mod = parser.OFPFlowMod(datapath=datapath,
                               priority=priority,
                               match=match,
                               instructions=inst,
                               idle_timeout=idle_timeout)

        datapath.send_msg(mod)

    #handle incoming packets
    @set_ev_cls(ofp_event.EventOFPPacketIn, MAIN_DISPATCHER)
    def packet_in_handler(self, ev):
        msg = ev.msg
        datapath = msg.datapath
        ofproto = datapath.ofproto
        parser = datapath.ofproto_parser

        dpid = datapath.id
        self.mac_to_port.setdefault(dpid, {})

        pkt = packet.Packet(msg.data)
        eth = pkt.get_protocol(ethernet.ethernet)

        dst = eth.dst
        src = eth.src
        in_port = msg.match['in_port']

        #learn which port this MAC address came from
        self.mac_to_port[dpid][src] = in_port

        #broadcast handling
        if dst == self.BROADCAST_MAC:
            current_time = time.time()

            #first time seeing this host
            if src not in self.broadcast_stats:
                self.broadcast_stats[src] = [1, current_time]
            else:
                count, start_time = self.broadcast_stats[src]

                #if within time window, increase count
                if current_time - start_time <= self.TIME_WINDOW:
                    count += 1
                else:
                    #reset count after time window
                    count = 1
                    start_time = current_time

                self.broadcast_stats[src] = [count, start_time]

            count, _ = self.broadcast_stats[src]

            #if too many broadcasts, drop packet
            if count > self.BROADCAST_THRESHOLD:
                self.logger.info(f"[BLOCK] Broadcast flood from {src}, count={count}")
                return

            #otherwise allow broadcast
            self.logger.info(f"[ALLOW] Broadcast from {src}, count={count}")
            out_port = ofproto.OFPP_FLOOD

        else:
            #normal forwarding
            if dst in self.mac_to_port[dpid]:
                out_port = self.mac_to_port[dpid][dst]
            else:
                out_port = ofproto.OFPP_FLOOD

        actions = [parser.OFPActionOutput(out_port)]

        #add flow rule for known destination
        if out_port != ofproto.OFPP_FLOOD:
            match = parser.OFPMatch(in_port=in_port, eth_dst=dst, eth_src=src)
            self.add_flow(datapath, priority=1, match=match, actions=actions)

        #send packet out
        out = parser.OFPPacketOut(datapath=datapath,
                                 buffer_id=msg.buffer_id,
                                 in_port=in_port,
                                 actions=actions,
                                 data=msg.data if msg.buffer_id == ofproto.OFP_NO_BUFFER else None)

        datapath.send_msg(out)