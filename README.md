# SDN Mininet based Simulation Project: Broadcast Traffic Control
### Problem Statement: Control excessive broadcast traffic in the network.
#### Tasks:
- Detect broadcast packets
- Limit flooding
- Install selective forwarding rules
- Evaluate improvement
---

### Setup
#### Mininet
``sudo mn --topo single,3 --controller=remote --switch ovsk,protocols=OpenFlow13``
- `sudo mn`: Runs Mininet
- `--topo single,3`
#### Controller
``````
