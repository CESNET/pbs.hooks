import pbs

cpuinfo_file = "/proc/cpuinfo"
resource_name = "cpu_flag"


try:
    e = pbs.event()

    if e.type == pbs.EXECHOST_STARTUP or e.type == pbs.EXECHOST_PERIODIC:

        try:
            with open(cpuinfo_file) as f:
                lines = f.readlines()
        except:
            pbs.logmsg(pbs.EVENT_DEBUG, "cpu_flag hook, failed to read %s" % cpuinfo_file)
            lines = []

        flags = None
        for line in lines:
            line = line.split(":")
            if line[0].strip() == "flags":
                line[1] = line[1].strip()
                flags = '"%s"' % ",".join(line[1].split())
                break

        if flags:
            vnl = pbs.event().vnode_list
            local_node = pbs.get_local_nodename()
            vnl[local_node].resources_available[resource_name] = flags
            pbs.logmsg(pbs.EVENT_DEBUG, "cpu_flag hook, reporting %s = %s" % (resource_name, flags))

except SystemExit:
    pass
except:
    e.reject("cpu_flag hook failed")
