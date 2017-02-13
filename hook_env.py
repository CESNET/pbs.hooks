import pbs
import re

try:
    e = pbs.event()

    if e.type == pbs.EXECJOB_BEGIN:
        j = e.job
        node=pbs.get_local_nodename()
        pbs.logmsg(pbs.EVENT_DEBUG, "env hook, node: %s" % node)
        pbs.logmsg(pbs.EVENT_DEBUG, "env hook, %s has exec_vnode: %s" % (j.id, str(j.exec_vnode)))
        
        resources = {}
        for i in str(j.exec_vnode).split("+"):
            i = i.replace("(","")
            i = i.replace(")","")

            node_i = i.split(":")[0].split(".")[0]

            if not node_i in resources.keys():
                resources[node_i] = {"ncpus":0, "mem":0}

            m = re.search('ncpus=([0-9]+)', i)
            if m:
                resources[node_i]["ncpus"] += int(m.group(1))

            m = re.search('mem=([0-9]+)kb', i)
            if m:
                resources[node_i]["mem"] += int(m.group(1)) * 1024

        pbs.logmsg(pbs.EVENT_DEBUG, "env hook, resources: %s" % str(resources))

        if node in resources.keys():
            j.Variable_List["PBS_RESC_MEM"] = resources[node]["mem"]
            j.Variable_List["TORQUE_RESC_MEM"] = resources[node]["mem"]

            j.Variable_List["PBS_NUM_PPN"] = resources[node]["ncpus"]
            j.Variable_List["PBS_NCPUS"] = resources[node]["ncpus"]
            j.Variable_List["TORQUE_RESC_PROC"] = resources[node]["ncpus"]

        total_mem = 0
        for node_i in resources.keys():
            total_mem += resources[node_i]["mem"]
        j.Variable_List["PBS_RESC_TOTAL_MEM"] = total_mem
        j.Variable_List["TORQUE_RESC_TOTAL_MEM"] = total_mem
        
        total_ncpus = 0
        for node_i in resources.keys():
            total_ncpus += resources[node_i]["ncpus"]
        j.Variable_List["PBS_RESC_TOTAL_PROCS"] = total_ncpus
        j.Variable_List["TORQUE_RESC_TOTAL_PROCS"] = total_ncpus        

        j.Variable_List["PBS_NUM_NODES"] = len(resources.keys())
        
        pbs.logmsg(pbs.EVENT_DEBUG, "env hook, new Variable_List: %s" % str(j.Variable_List))
        
except SystemExit:
    pass
except:
    e.reject("env hook failed")
