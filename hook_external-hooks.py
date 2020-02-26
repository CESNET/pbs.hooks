import pbs
import sys
import socket 
import subprocess, os

DIR_PROLOGUE="/var/spool/pbs/mom_priv/prologue.d/"
DIR_EPILOGUE="/var/spool/pbs/mom_priv/epilogue.d/"
DIR_END="/var/spool/pbs/mom_priv/end.d/"

if not DIR_PROLOGUE.endswith("/"):
    DIR_PROLOGUE += "/"

if not DIR_EPILOGUE.endswith("/"):
    DIR_EPILOGUE += "/"

if not DIR_END.endswith("/"):
    DIR_END += "/"

def is_executable(fpath):
    return os.path.isfile(fpath) and os.access(fpath, os.X_OK)

def run_file(fpath):
    try:
        pbs.logmsg(pbs.EVENT_DEBUG, "external hook started: %s" % fpath)
        command = fpath
        #new_env = os.environ.copy()
        new_env = {}
        new_env['JOBID'] = j.id
        new_env['USER'] = j.euser #Job_Owner.split("@")[0]
        new_env['GROUP'] = j.egroup
        new_env['HOSTNAME'] = socket.gethostname()
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=new_env)
        (out, err) = proc.communicate()
        pbs.logmsg(pbs.EVENT_DEBUG, "external hook %s stdout: %s stderr: %s" % (fpath, str(out), str(err)))
        pbs.logmsg(pbs.EVENT_DEBUG, "external hook ended with exitcode: %d" % proc.returncode)
    except Exception as error:
        pbs.logmsg(pbs.EVENT_DEBUG, "external hook %s failed: %s" % (fpath, str(error)))

def run_dir(path):
    if not os.path.isdir(path):
        pbs.logmsg(pbs.EVENT_DEBUG, "no external hook dir: %s" % path)
        e.accept()

    for file in sorted(os.listdir(path)):
        fpath = path + file

        if is_executable(fpath):
            run_file(fpath)

try:
    e = pbs.event()

    if e.type == pbs.EXECJOB_PROLOGUE:
        j = e.job
        run_dir(DIR_PROLOGUE)

    if e.type == pbs.EXECJOB_EPILOGUE:
        j = e.job
        run_dir(DIR_EPILOGUE)

    if e.type == pbs.EXECJOB_END:
        j = e.job
        run_dir(DIR_END)
        
except SystemExit:
    pass
except:
    e.reject("external-hooks hook failed")
