import pbs
import re
import math

try:
    e = pbs.event()
    if e.type in [pbs.QUEUEJOB, pbs.MODIFYJOB]:
        j = e.job
        if "nodes" in j.Resource_List.keys():
            pbs.logmsg(pbs.EVENT_DEBUG,"Resource_List: " + str(j.Resource_List.keys()))
            e.reject("Old syntax rejected. Please use 'select' syntax.")

        #  mpiprocs = ncpus/ompthreads
        #  ompthreads = ncpus/mpiprocs
        if "select" in j.Resource_List.keys():
            newselect = []
            for i in str(j.Resource_List["select"]).split("+"):
                ncpus = 0
                ompthreads = 0
                mpiprocs = 0

                m = re.search('.*os=debian([0-9]+).*', i)
                if m:
                    debversion = int(m.group(1))
                    if (debversion < 10):
                        e.reject("Unsupported debian version.")

                m = re.search('.*ncpus=([0-9]+).*', i)
                if m:
                    ncpus = int(m.group(1))

                m = re.search('.*mpiprocs=([0-9]+).*', i)
                if m:
                    mpiprocs = int(m.group(1))

                m = re.search('.*ompthreads=([0-9]+).*', i)
                if m:
                    ompthreads = int(m.group(1))

                if not ncpus:
                    if not mpiprocs and not ompthreads:
                        newselect.append(i)
                        continue
                    else:
                        if mpiprocs and ompthreads:
                            ncpus = mpiprocs * ompthreads
                        if not mpiprocs:
                            ncpus = ompthreads
                        if not ompthreads:
                            ncpus = mpiprocs
                        i += ":ncpus=%d" % ncpus

                if not mpiprocs and not ompthreads:
                    mpiprocs = ncpus
                    ompthreads = 1
                    i += ":mpiprocs=%d:ompthreads=1" % mpiprocs

                elif mpiprocs and not ompthreads:
                    try:
                        ompthreads = math.ceil(ncpus/mpiprocs)
                        if not ompthreads:
                            ompthreads = 1
                    except:
                        ompthreads = 1
                    i += ":ompthreads=%d" % ompthreads

                elif not mpiprocs and ompthreads:
                    try:
                        mpiprocs = math.ceil(ncpus/ompthreads)
                        if not mpiprocs:
                            mpiprocs = 1
                    except:
                        mpiprocs = 1
                    i += ":mpiprocs=%d" % mpiprocs

                newselect.append(i)

                if (mpiprocs * ompthreads > ncpus):
                    e.reject("Incorrect select: mpiprocs * ompthreads > ncpus. Please, check qsub parameters.")

            pbs.logmsg(pbs.LOG_DEBUG, "Old select: %s" % str(j.Resource_List))
            j.Resource_List["select"] = pbs.select("+".join(newselect))
            pbs.logmsg(pbs.LOG_DEBUG, "New select: %s" % str(j.Resource_List))

        if "ncpus" in j.Resource_List.keys() and not "mpiprocs" in j.Resource_List.keys():
            j.Resource_List["mpiprocs"] = j.Resource_List["ncpus"]



except SystemExit:
    pass
except Exception as err:
    e.reject("job-enqueued hook failed with error: %s" % str(err))
