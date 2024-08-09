import pbs
import re
import math

queue_name = "uv18"
ncpus_divider = 18

try:
    e = pbs.event()
    if e.type == pbs.MOVEJOB:
        j = e.job

        if str(j.queue).split("@")[0] != queue_name:
            e.accept()

        e.reject("Can not move the job into the queue '%s'. Please, submit directly." % queue_name)

    if e.type in [pbs.QUEUEJOB, pbs.MODIFYJOB]:
        j = e.job

        if str(j.queue).split("@")[0] != queue_name:
            e.accept()

        reqncpus = 0

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

                reqncpus += multichunk * ncpus
                
                if ncpus % ncpus_divider != 0:
                    e.reject("The number of CPUs of each chunk must be divisible by %d" % ncpus_divider)

        else:
            if "ncpus" in j.Resource_List.keys():
                reqncpus = j.Resource_List["ncpus"]

        if reqncpus == 0:
            e.reject("The queue '%s' requires ncpus specification." % queue_name)

        if reqncpus % ncpus_divider != 0:
            e.reject("The number of CPUs must be divisible by %d" % ncpus_divider)

except SystemExit:
    pass
except Exception as err:
    e.reject("uvsize hook failed with error: %s" % str(err))
