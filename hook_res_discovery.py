import pbs
import os


class Discovery(object):
    hook_name = "res_discovery"
    hook_events = {}
    vnl = None
    local_node = None



    def __init__(self, pbs_event):
        self.hook_events = { pbs.EXECHOST_STARTUP: self.__setallresources_handler,
                             pbs.EXECHOST_PERIODIC: self.__setallresources_handler,
                           }
        self.e = pbs_event
        self.vnl = pbs.event().vnode_list
        self.local_node = pbs.get_local_nodename()

        if self.vnl == None or self.local_node == None:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, failed to get local_node or vnl" % self.hook_name)
            self.e.accept()



    def __setallresources_handler(self):
        if self.getandset_cgroups() == False:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, failed to get and set cgroups resource" % self.hook_name)

        if self.getandset_cpu_flag() == False:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, failed to get and set cpu_flag resource" % self.hook_name)

        if self.getandset_os() == False:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, failed to get and set os resource" % self.hook_name)



    def run(self):
        if self.e.type in self.hook_events.keys():
            self.hook_events[self.e.type]()
        else:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, invalid event" % self.hook_name)



    def accept(self):
        self.e.accept()



    ################################################
    # cgroups
    ################################################
    def getandset_cgroups(self):
        try:
            if os.path.isdir("/sys/fs/cgroup/cpuset") and os.path.isdir("/sys/fs/cgroup/memory"):
                self.vnl[self.local_node].resources_available["cgroups"] = True
                pbs.logmsg(pbs.EVENT_DEBUG, "%s, resource cgroups set to: True" % self.hook_name)
            else:
                self.vnl[self.local_node].resources_available["cgroups"] = None
                pbs.logmsg(pbs.EVENT_DEBUG, "%s, resource cgroups unset" % self.hook_name)
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, getandset_cgroups error: %s" % (self.hook_name, str(err)))
            return False
        return True


    ################################################
    # cpu_flag
    ################################################
    def getandset_cpu_flag(self):
        cpuinfo_file = "/proc/cpuinfo"

        try:
            with open(cpuinfo_file) as f:
                lines = f.readlines()
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, getandset_cpu_flag error: %s" % (self.hook_name, str(err)))
            return False

        flags = None
        for line in lines:
            line = line.split(":")
            if line[0].strip() == "flags":
                line[1] = line[1].strip()
                flags = "%s" % ",".join(line[1].split())
                break

        if flags:
            self.vnl[self.local_node].resources_available["cpu_flag"] = flags
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, resource cpu_flag set to: %s" % (self.hook_name, flags))

        return True



    ################################################
    # os
    ################################################
    def getandset_os(self):
        files_to_check = ["/etc/os-release"]
        lines = []
        os = ""
        version = ""
        try:
            for file in files_to_check:
                with open(file) as f:
                    l = f.readlines()
                lines += l
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, getandset_os error: %s" % (self.hook_name, str(err)))
            pass

        try:
            for line in lines:
                line = line.split("=");
                if line[0] == "ID":
                    os = line[1].replace('"','').strip()
                if line[0] == "VERSION_ID":
                    version = line[1].replace('"','').strip()
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, getandset_os error: %s" % (self.hook_name, str(err)))
            return False

        if os == "":
            return False
        else:
            self.vnl[self.local_node].resources_available["os"] = os+version
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, resource os set to: %s" % (self.hook_name, os+version))
        return True



try:
    e = pbs.event()
    discovery = Discovery(e)    
    discovery.run()
    discovery.accept()                
except SystemExit:
    pass
except Exception as err:
    e.reject("res_discovery, hook failed: %s " % str(err))
