import pbs
import re

max_walltime = 172800
min_mem = 524288000
large_queue = "large_mem"

try:
    e = pbs.event()
    if e.type in [pbs.QUEUEJOB, pbs.MODIFYJOB]:
        j = e.job
        if str(j.queue) in ["default", "", large_queue]:
            if j.Resource_List["walltime"] != None:
                splitwalltime = str(j.Resource_List["walltime"]).split(":")
                sec = 0
                min2sec = 0
                hour2sec = 0

                if len(splitwalltime) == 1:
                    sec = int(splitwalltime[0])

                if len(splitwalltime) == 2:
                    min2sec = int(splitwalltime[0]) * 60
                    sec = int(splitwalltime[1])

                if len(splitwalltime) == 3:
                    hour2sec = int(splitwalltime[0]) * 60 * 60
                    min2sec = int(splitwalltime[1]) * 60
                    sec = int(splitwalltime[2])

                if (sec + min2sec + hour2sec) > max_walltime:
                    if str(j.queue) in ["default", ""]:
                        e.accept()
                    if str(j.queue) == large_queue:
                        e.reject(f"max allwed waltime is {max_walltime} seconds")

            if "select" in j.Resource_List.keys():
                mem = 0 # KB
                for i in str(j.Resource_List["select"]).split("+"):

                    m = re.search('.*mem=([0-9]+)(kb|KB|Kb|kB)', i)
                    if m:
                        mem += int(m.group(1))

                    m = re.search('.*mem=([0-9]+)(mb|MB|Mb|mB)', i)
                    if m:
                        mem += (int(m.group(1)) * 1024)

                    m = re.search('.*mem=([0-9]+)(gb|GB|Gb|gB)', i)
                    if m:
                        mem += (int(m.group(1)) * 1024 * 1024)

                    m = re.search('.*mem=([0-9]+)(tb|TB|Tb|tB)', i)
                    if m:
                        mem += (int(m.group(1)) * 1024 * 1024 * 1024)

                    m = re.search('.*cluster=([a-z]+).*', i)
                    if m:
                        if not m.group(1).startswith("elwe"):
                            e.accept()

                    m = re.search('.*cl_([a-z]+)=[Tt]+rue.*', i)
                    if m:
                        if not m.group(1).startswith("elwe"):
                            e.accept()

                    m = re.search('.*vnode=([a-z]+).*', i)
                    if m:
                        if not m.group(1).startswith("elwe"):
                            e.accept()

                    m = re.search('.*scratch_shared=([0-1]+).*', i)
                    if m:
                        e.accept()

                if e.type == pbs.QUEUEJOB:
                    if str(j.queue) == large_queue and mem < min_mem:
                        e.reject(f"mem too small for {large_queue}")

                    if str(j.queue) in ["default", ""] and mem >= min_mem:
                        j.queue = pbs.server().queue(large_queue)

                if e.type == pbs.MODIFYJOB:
                    if str(j.queue) == large_queue and  mem < min_mem:
                        e.reject(f"can not modify job, memory too small for '{large_queue}', please resubmit")

                    if str(j.queue) in ["default", ""] and  mem >= min_mem:
                        e.reject("can not modify job, memory too large, please resubmit")
except SystemExit:
    pass
except Exception as err:
    e.reject("meta-pbs_large_mem hook failed with error: %s" % str(err))
