import pbs
import re

try:
    e = pbs.event()
    if e.type in [pbs.QUEUEJOB, pbs.MODIFYJOB]:
        j = e.job

        if str(j.queue).split("@")[0] != "gpu8":
            e.accept()

        msg_reject = "Only ncpus=x8:ngpus=x jobs are allowed."

        ncpus = 0
        ngpus = 0

        if "select" in j.Resource_List.keys():
            for i in str(j.Resource_List["select"]).split("+"):

                m = re.search('.*ncpus=([0-9]+).*', i)
                if m:
                    ncpus += int(m.group(1))

                m = re.search('.*ngpus=([0-9]+).*', i)
                if m:
                    ngpus += int(m.group(1))

        if "ncpus" in j.Resource_List.keys():
            ncpus = j.Resource_List["ncpus"]

        if "ngpus" in j.Resource_List.keys():
            ngpus = j.Resource_List["ngpus"]

        if ncpus == 0 or ngpus == 0:
            e.reject(msg_reject)

        if ncpus % 8 != 0:
            e.reject(msg_reject)

        if ncpus != ngpus * 8:
            e.reject(msg_reject)

        e.accept()

except SystemExit:
    pass
except Exception as err:
    e.reject("gpu8 hook failed with error: %s" % str(err))
