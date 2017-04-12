import pbs
import os

try:
    e = pbs.event()

    if e.type == pbs.EXECHOST_STARTUP or e.type == pbs.EXECHOST_PERIODIC:
        vnl = pbs.event().vnode_list
        local_node = pbs.get_local_nodename()

        if os.path.isdir("/sys/fs/cgroup/cpuset") and os.path.isdir("/sys/fs/cgroup/memory"):
            vnl[local_node].resources_available["cgroups"] = True
        else:
            vnl[local_node].resources_available["cgroups"] = None

except:
    pass
