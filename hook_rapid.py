import pbs
import os

default_queue = "default"
interactive_queue = "interactive"
max_duration = pbs.duration("48:00:00")

def check_interactive_suitable(j):
    if not j.interactive:
        return False

    if j.Resource_List["walltime"] and pbs.duration(j.Resource_List["walltime"]) > max_duration:
        return False

    return True


try:
    e = pbs.event()
    if e.type == pbs.QUEUEJOB:
        j = e.job

        # checking directly submited job suitability (in interactive queue)
        if str(j.queue) == interactive_queue:
            if check_interactive_suitable(j):
                e.accept()
            else:
                e.reject("job is not suitable for the queue: %s" % str(interactive_queue))

        # move suitable jobs to interactive queue
        if str(j.queue) == "" or str(j.queue) == default_queue:
            if check_interactive_suitable(j):
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

            os.environ["PBSPRO_IGNORE_KERBEROS"] = ""
            os.environ['PATH'] += ":/opt/pbs/bin/"
            os.system(str.format("qmove %s %s" % (default_queue, j.id)))

        e.accept()

except SystemExit:
    pass
except Exception as err:
    e.reject("interactive hook failed, error: %s" % str(err))
