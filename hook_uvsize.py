import pbs
import re
import math

route_resource_name = "uvsize"
default_mem_chunk = 409600 # 400mb in kb

min_ncpus_for_large = 144 #ncpus
min_mem_for_large = 1073741824 # 1tb in kb

units = ["kb", "mb", "gb", "tb"]   

try:
    e = pbs.event()
    if e.type == pbs.QUEUEJOB:
        j = e.job

        if str(j.queue) != "uv":
			e.accept()

        reqncpus = 0
        reqmem = 0

        j.Resource_List[route_resource_name] = "small"

        if "select" in j.Resource_List.keys():
            for chunk in str(j.Resource_List["select"]).split("+"):

                multichunk = 1
                res = re.match("([0-9]*)(:)?.*", chunk)
                if res and res.group(1):
                    multichunk = int(res.group(1))

                ncpus = 1
                res = re.match(".*ncpus=([0-9]*)(:)?.*", chunk)
                if res and res.group(1):
                    ncpus = int(res.group(1))

                mem = default_mem_chunk
                res = re.match(".*mem=([0-9]*)(:)?.*", chunk)
                if res and res.group(1):
                    mem = int(res.group(1))

                memunit="kb"
                res = re.match(".*mem=[0-9]*((%s)?).*" % '|'.join(units), chunk.lower())
                if res and res.group(1):
                    memunit = str(res.group(1))

                mem *= math.pow(1024, units.index(memunit))

                reqncpus += multichunk * ncpus
                reqmem += multichunk * mem

        else:
            if "ncpus" in j.Resource_List.keys():
                reqncpus = j.Resource_List["ncpus"]

            if "mem" in j.Resource_List.keys():

                mem = default_mem_chunk
                res = re.match("([0-9]*)(:)?.*", str(j.Resource_List["mem"]))
                if res and res.group(1):
                    mem = int(res.group(1))

                memunit="kb"
                res = re.match("[0-9]*((%s)?).*" % '|'.join(units), str(j.Resource_List["mem"]).lower())
                if res and res.group(1):
                    memunit = str(res.group(1))

                reqmem = mem * math.pow(1024, units.index(memunit))

        if not reqncpus:
            reqncpus = 1

        if not reqmem:
            reqmem = default_mem_chunk

        if reqncpus >= min_ncpus_for_large or reqmem >= min_mem_for_large:
            j.Resource_List[route_resource_name] = "large"

except SystemExit:
    pass
except Exception as err:
    e.reject("uvsize hook failed with error: %s" % str(err))
