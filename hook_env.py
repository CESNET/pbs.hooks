import pbs
import re

try:
    e = pbs.event()

    if e.type == pbs.EXECJOB_BEGIN:
        j = e.job
        host=e.requestor_host.split(".")[0]
        pbs.logmsg(pbs.EVENT_DEBUG, "env hook, host: %s" % host)
        pbs.logmsg(pbs.EVENT_DEBUG, "env hook, %s has exec_vnode: %s" % (j.id, str(j.exec_vnode)))
        
        resources = {}
        for i in str(j.exec_vnode).split("+"):
            i = i.replace("(","")
            i = i.replace(")","")

            host_i = i.split(":")[0].split(".")[0]

            if not host_i in resources.keys():
                resources[host_i] = {"ncpus":0, "mem":0}

            m = re.search('ncpus=([0-9]+)', i)
            if m:
                resources[host_i]["ncpus"] += int(m.group(1))

            m = re.search('mem=([0-9]+)kb', i)
            if m:
                resources[host_i]["mem"] += int(m.group(1)) * 1024

        pbs.logmsg(pbs.EVENT_DEBUG, "env hook, resources: %s" % str(resources))

        if host in resources.keys():
            j.Variable_List["PBS_RESC_MEM"] = resources[host]["mem"]
            j.Variable_List["TORQUE_RESC_MEM"] = resources[host]["mem"]

            j.Variable_List["PBS_NUM_PPN"] = resources[host]["ncpus"]
            j.Variable_List["PBS_NCPUS"] = resources[host]["ncpus"]
            j.Variable_List["TORQUE_RESC_PROC"] = resources[host]["ncpus"]

        total_mem = 0
        for host_i in resources.keys():
            total_mem += resources[host_i]["mem"]
        j.Variable_List["PBS_RESC_TOTAL_MEM"] = total_mem
        j.Variable_List["TORQUE_RESC_TOTAL_MEM"] = total_mem
        
        total_ncpus = 0
        for host_i in resources.keys():
            total_ncpus += resources[host_i]["ncpus"]
        j.Variable_List["PBS_RESC_TOTAL_PROCS"] = total_ncpus
        j.Variable_List["TORQUE_RESC_TOTAL_PROCS"] = total_ncpus        

        j.Variable_List["PBS_NUM_NODES"] = len(resources.keys())
        
        pbs.logmsg(pbs.EVENT_DEBUG, "env hook, new Variable_List: %s" % str(j.Variable_List))
        
except SystemExit:
    pass
except:
    e.reject("env hook failed")
