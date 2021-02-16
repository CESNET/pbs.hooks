import pbs
import re
import math

try:
    e = pbs.event()
    if e.type == pbs.QUEUEJOB:
        j = e.job

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

            if mem >= 524288000:
                j.queue = pbs.server().queue("large_mem")

except SystemExit:
    pass
except Exception as err:
    e.reject("meta-pbs_large_mem hook failed with error: %s" % str(err))
