import pbs
import re

try:
    e = pbs.event()
    if e.type == pbs.QUEUEJOB:
        j = e.job
        if "nodes" in j.Resource_List.keys():
            pbs.logmsg(pbs.EVENT_DEBUG,"Resource_List: " + str(j.Resource_List.keys()))
            e.reject("Old syntax rejected. Please use 'select' syntax.")

        #  mpiprocs se ma rovnat ncpus
        if "select" in j.Resource_List.keys():
            newselect = []
            for i in str(j.Resource_List["select"]).split("+"):
                m = re.search('.*ncpus=([0-9]+).*', i)
                if m:
                    ncpus = m.group(1)
                else:
                    newselect.append(i)
                    continue

                m = re.search('.*mpiprocs=([0-9]+).*', i)
                if not m:
                    i += ":mpiprocs=" + str(ncpus)

                newselect.append(i)

            pbs.logmsg(pbs.LOG_DEBUG, "Old select: %s" % str(j.Resource_List))
            j.Resource_List["select"] = pbs.select("+".join(newselect))
            pbs.logmsg(pbs.LOG_DEBUG, "New select: %s" % str(j.Resource_List))


except SystemExit:
    pass
except:
    e.reject("job-enqueued hook failed")
