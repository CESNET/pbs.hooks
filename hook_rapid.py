import pbs
import os

#default interactive
default_queue = "default"
interactive_queue = "interactive"
max_duration = pbs.duration("48:00:00")
allowed_hosts = ["ondemand.grid.cesnet.cz"]

def check_interactive_suitable(j, r):
    if not j.interactive and not r in allowed_hosts:
        return False

    if j.Resource_List["walltime"] and pbs.duration(j.Resource_List["walltime"]) > max_duration:
        return False

    return True

def move_job(queue, jid):
    os.environ["PBSPRO_IGNORE_KERBEROS"] = ""
    os.environ['PATH'] += ":/opt/pbs/bin/"
    os.system(str.format("qmove %s %s" % (queue, jid)))

try:
    e = pbs.event()
    if e.type == pbs.QUEUEJOB:
        j = e.job
        requestor_host = e.requestor_host

        # checking directly submited job suitability (in interactive queue)
        if str(j.queue) == interactive_queue:
            if check_interactive_suitable(j, requestor_host):
                e.accept()
            else:
                e.reject("job is not suitable for the queue: %s" % str(interactive_queue))

        # move suitable jobs to interactive queue
        if str(j.queue) == "" or str(j.queue) == default_queue:
            if check_interactive_suitable(j, requestor_host):
                q = pbs.server().queue(interactive_queue)
                j.queue = q

        e.accept()

    if e.type == pbs.PERIODIC:
        q = pbs.server().queue(interactive_queue)
        for j in q.jobs():
            if j.job_state != pbs.JOB_STATE_QUEUED:
                continue

            if j.comment == None:
                continue

            move_job(default_queue, j.id)

        e.accept()

except SystemExit:
    pass
except Exception as err:
    e.reject("rapid hook failed, error: %s" % str(err))
