import pbs
import os
import json
import re
import subprocess


class Discovery(object):
    hook_name = "res_discovery"
    hook_events = {}
    vnl = None
    local_node = None
    
    cgroups_types = ["cpuacct", "cpuset", "memory", "memsw"]    
    exclude_hosts = {}
    spec = {}


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

        if self.getandset_osfamily() == False:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, failed to get and set osfamily resource" % self.hook_name)

        if self.getandset_cuda_version() == False:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, failed to get and set cuda_version resource" % self.hook_name)

        if self.getandset_cpu_vendor() == False:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, failed to get and set cpu_vendor resource" % self.hook_name)

        if self.getandset_gpu_mem() == False:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, failed to get and set gpu_mem resource" % self.hook_name)

        if self.getandset_spec() == False:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, failed to get and set spec resource" % self.hook_name)

    def run(self):
        if self.e.type in self.hook_events.keys():
            self.hook_events[self.e.type]()
        else:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, invalid event" % self.hook_name)



    def accept(self):
        self.e.accept()



    def parse_cfg(self):
        config = {}
        if 'PBS_HOOK_CONFIG_FILE' in os.environ:
            config_file = os.environ["PBS_HOOK_CONFIG_FILE"]
            try:
                config = json.loads(open(config_file, 'r').read())
            except:
                pbs.logmsg(pbs.EVENT_DEBUG, "%s, failed to open config file" % self.hook_name)
                config = {}
                return False

        try:
            if "cgroup_exclude_hosts" in config.keys():
                self.exclude_hosts["general"] = list(config["cgroup_exclude_hosts"])
            else:
                self.exclude_hosts["general"] = {}
                
                
            if "cgroup" in config.keys():
                for flag in self.cgroups_types:
                    if flag in config["cgroup"].keys() and "exclude_hosts" in config["cgroup"][flag].keys():
                        self.exclude_hosts[flag] = list(config["cgroup"][flag]["exclude_hosts"])

            if "spec" in config.keys():
                for noder in config["spec"].keys():
                    self.spec[noder] = config["spec"][noder]

        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, failed to parse config '%s'" % (self.hook_name, err))
            return False
            
        return True



    ################################################
    # cgroups
    ################################################
    def getandset_cgroups(self):
        flags = []
        
        if not self.parse_cfg():
            return False

        if self.local_node in self.exclude_hosts["general"]:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, cgroups flags on %s: %s" % (self.hook_name, self.local_node, str(flags))) 
            self.vnl[self.local_node].resources_available["cgroups"] = None
            return True
		
        for flag in self.cgroups_types:
            if os.path.isdir("/sys/fs/cgroup/%s" % flag) and self.local_node not in self.exclude_hosts[flag]:
                flags.append(flag)
        
        try:
            self.vnl[self.local_node].resources_available["cgroups"] = ",".join(flags)
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, cgroups flags on %s: %s" % (self.hook_name, self.local_node, str(flags)))            
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
        version_aliases = {"rhel7":"centos7"}
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
                    version = line[1].replace('"','').strip().split(".")[0]
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, getandset_os error: %s" % (self.hook_name, str(err)))
            return False

        if os == "":
            return False
        else:
            res_value = os+version
            if res_value in version_aliases.keys():
                res_value = version_aliases[res_value]
            self.vnl[self.local_node].resources_available["os"] = res_value
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, resource os set to: %s" % (self.hook_name, res_value))
        return True

    ################################################
    # osfamily
    ################################################
    def getandset_osfamily(self):
        files_to_check = ["/etc/os-release"]
        lines = []
        version_aliases = {"centos":"redhat", "rhel":"redhat"}
        osfamily = ""
        try:
            for file in files_to_check:
                with open(file) as f:
                    l = f.readlines()
                lines += l
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, getandset_osfamily error: %s" % (self.hook_name, str(err)))
            pass

        try:
            for line in lines:
                line = line.split("=");
                if line[0] == "ID":
                    osfamily = line[1].replace('"','').strip()
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, getandset_osfamily error: %s" % (self.hook_name, str(err)))
            return False

        if osfamily == "":
            return False
        else:
            res_value = osfamily
            if res_value in version_aliases.keys():
                res_value = version_aliases[res_value]
            self.vnl[self.local_node].resources_available["osfamily"] = res_value
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, resource osfamily set to: %s" % (self.hook_name, res_value))
        return True

    ################################################
    # cuda_version
    ################################################
    def getandset_cuda_version(self):
        cuda_version = ""
        cmd = "/software/cuda/8.0/samples/bin/x86_64/linux/release/drv_ver"
        try:
            result = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
            cuda_version = result.communicate()[0].decode("utf-8").strip()
            returncode = result.returncode

            if returncode != 0:
                cuda_version=""
        except:
            cuda_version = ""

        if cuda_version and len(cuda_version):
            self.vnl[self.local_node].resources_available["cuda_version"] = cuda_version
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, resource cuda_version set to: %s" % (self.hook_name, cuda_version))
        return True

    ################################################
    # cpu_vendor
    ################################################
    def getandset_cpu_vendor(self):
        files_to_check = ["/proc/cpuinfo"]
        lines = []
        version_aliases = {"GenuineIntel":"intel", "AuthenticAMD":"amd"}
        cpu_vendor = ""
        try:
            for file in files_to_check:
                with open(file) as f:
                    l = f.readlines()
                lines += l
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, getandset_cpu_vendor error: %s" % (self.hook_name, str(err)))
            pass

        try:
            for line in lines:
                line = line.split(":");
                if line[0].strip() == "vendor_id":
                    cpu_vendor = line[1].strip()
                    break
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, getandset_cpu_vendor error: %s" % (self.hook_name, str(err)))
            return False

        if cpu_vendor == "":
            return False
        else:
            res_value = cpu_vendor
            if res_value in version_aliases.keys():
                res_value = version_aliases[res_value]
            self.vnl[self.local_node].resources_available["cpu_vendor"] = res_value
            pbs.logmsg(pbs.EVENT_DEBUG, "%s, resource cpu_vendor set to: %s" % (self.hook_name, res_value))
        return True

    ################################################
    # gpu_mem
    ################################################
    def getandset_gpu_mem(self):
        cmd = ['nvidia-smi', '--query-gpu=memory.total', '--format=csv', '-i', '0']
        try:
            result = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE)
            nvidia_smi = result.communicate()[0].decode("utf-8").strip()
            returncode = result.returncode

            if returncode != 0:
                nvidia_smi=""
        except:
            nvidia_smi = ""

        gpu_mem = 0
        if len(nvidia_smi) > 0:
            for line in nvidia_smi.split('\n'):
                l = line.split()
                if len(l) == 2 and l[1] == "MiB":
                    try:
                        gpu_mem = int(l[0])
                    except:
                        gpu_mem = 0
            if gpu_mem > 0:
               self.vnl[self.local_node].resources_available["gpu_mem"] = pbs.size("%dmb" % gpu_mem)
               pbs.logmsg(pbs.EVENT_DEBUG, "%s, resource gpu_mem set to: %d mb" % (self.hook_name, gpu_mem))
               return True

        return False

    ################################################
    # spec
    ################################################
    def getandset_spec(self):
        if not self.parse_cfg():
            return False

        for noder in self.spec.keys():
            if re.search(noder, self.local_node):
                self.vnl[self.local_node].resources_available["spec"] = self.spec[noder]
                pbs.logmsg(pbs.EVENT_DEBUG, "%s, resource spec set to: %f" % (self.hook_name, self.spec[noder]))
                return True

        return False

try:
    e = pbs.event()
    discovery = Discovery(e)
    discovery.run()
    discovery.accept()                
except SystemExit:
    pass
except Exception as err:
    e.reject("res_discovery, hook failed: %s " % str(err))
