import pbs
import os
import string
import re

def size_to_int(mem):
    """
    Converts the value of type pbs.size to integer amount of bytes.
    Parameter @mem is expected to be of type pbs.size or None.
    """
	
    # Unset values should be treated as zero
    if not mem:
        return 0
	
    mem = str(mem)
	
    # remove byte suffix
    numeric_string = mem[:-1].lower()
    # suffix will contain the character before "b" or an empty string
    suffix = mem[-2:-1].lower()
	
    # the only suffix is "b", no need to multiply by 1024 ** n
    if suffix in string.digits:
        suffix = ""
    else:
        numeric_string = numeric_string[:-1]
	
    value = int(numeric_string)
	
    if suffix == "y":
        value *= 1024 ** 8
    elif suffix == "z":
        value *= 1024 ** 7
    elif suffix == "e":
        value *= 1024 ** 6
    elif suffix == "p":
        value *= 1024 ** 5
    elif suffix == "t":
        value *= 1024 ** 4
    elif suffix == "g":
        value *= 1024 ** 3
    elif suffix == "m":
        value *= 1024 ** 2
    elif suffix == "k":
        value *= 1024 ** 1
    return value

try:
    e = pbs.event()
    if e.type == pbs.PERIODIC:
        pbs.logmsg(pbs.LOG_DEBUG, "fairshare periodic started")
        infrastructure_ncpus = int()
        infrastructure_mem = int()
        infrastructure_ngpus = int()
        infrastructure_scratch_local = int()
        infrastructure_scratch_shared = int()
        infrastructure_scratch_ssd = int()

        # We divide memory values by 1024 because the implicit conversion of pbs.size
        # during division in the scheduling formula converts the value to kilobytes
        vnodes = pbs.server().vnodes()
        for vnode in vnodes:
            if vnode.resources_available["ncpus"]:
                infrastructure_ncpus += int(vnode.resources_available["ncpus"])
            infrastructure_mem += size_to_int(vnode.resources_available["mem"]) / 1024
            if vnode.resources_available["ngpus"]:
                infrastructure_ngpus += int(vnode.resources_available["ngpus"])
            infrastructure_scratch_local += size_to_int(vnode.resources_available["scratch_local"]) / 1024
            infrastructure_scratch_shared += size_to_int(vnode.resources_available["scratch_shared"]) / 1024
            infrastructure_scratch_ssd += size_to_int(vnode.resources_available["scratch_ssd"]) / 1024

        # The resources cannot be set directly so we have
        # to call os.system and set them via qmgr
        os.environ["PBSPRO_IGNORE_KERBEROS"] = ""
        os.environ['PATH'] += ":/opt/pbs/bin/"
        os.system(str.format("qmgr -c \"set server resources_default.infrastructure_ncpus = {}\"", int(infrastructure_ncpus)))
        os.system(str.format("qmgr -c \"set server resources_default.infrastructure_mem = {}kb\"", int(infrastructure_mem)))
        os.system(str.format("qmgr -c \"set server resources_default.infrastructure_ngpus = {}\"", int(infrastructure_ngpus)))
        os.system(str.format("qmgr -c \"set server resources_default.infrastructure_scratch_local = {}kb\"", int(infrastructure_scratch_local)))
        os.system(str.format("qmgr -c \"set server resources_default.infrastructure_scratch_shared = {}kb\"", int(infrastructure_scratch_shared)))
        os.system(str.format("qmgr -c \"set server resources_default.infrastructure_scratch_ssd = {}kb\"", int(infrastructure_scratch_ssd)))

    if e.type == pbs.EXECJOB_BEGIN:
        job = pbs.event().job
        if job.in_ms_mom():
            f_mem = job.Resource_List["mem"]
            if f_mem == None:
                f_mem = pbs.size('10mb')
            job.resources_used["fairshare_mem"] = f_mem

            try:
                node = pbs.server().vnode(pbs.get_local_nodename())

                f_spec = node.resources_available['spec']

                if f_spec == None:
                    f_spec = 1.0

                job.resources_used["fairshare_spec"] = f_spec
            except Exception as err:
                job.resources_used["fairshare_spec"] = 1.0

except SystemExit:
    pass
except Exception as err:
    e.reject("fairshare hook failed: (%s)" % str(err))
