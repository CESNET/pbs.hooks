import pbs
import os
import copy
import re
import subprocess

class Singularity(object):
    e = None
    j = None
    jid = None
    image = None
    is_running = False
    job_file = None
    container_type = None #interactive, service, script, executable
    hook_events = {}



    def __init__(self, pbs_event):
        self.hook_events = { pbs.QUEUEJOB: self.__queuejob_handler,
                             pbs.MODIFYJOB: self.__queuejob_handler,
                             pbs.EXECJOB_LAUNCH: self.__execjob_launch_handler,
                             pbs.EXECJOB_END: self.__execjob_end_handler,
                           }

        self.e = pbs_event
        self.j = self.e.job
        self.jid = self.j.id



    def __queuejob_handler(self):
        pbs.logmsg(pbs.LOG_DEBUG, 'Singularity queuejob handler start')

        newselect = []
        if "select" in self.j.Resource_List.keys():
            for i in str(self.j.Resource_List["select"]).split("+"):
                if re.search("singularity=[Tt]{1}rue", i):
                    newselect.append(i)
                    continue
                newselect.append(i + ":singularity=true")
        else:
            newselect.append("singularity=true")

        pbs.logmsg(pbs.LOG_DEBUG, f'Singularity old select: {str(self.j.Resource_List)}')
        self.j.Resource_List["select"] = pbs.select("+".join(newselect))
        pbs.logmsg(pbs.LOG_DEBUG, f'Singularity new select: {str(self.j.Resource_List)}')



    def __execjob_launch_handler(self):
        pbs.logmsg(pbs.LOG_DEBUG, 'Singularity execjob_launch handler start')

        if not self.check_container_running():
            self.create_container()

        if self.j.interactive and "PBS_TASKNUM" in self.e.env and self.e.env["PBS_TASKNUM"] == "1":
            self.container_type = "interactive"
            pbs.logmsg(pbs.LOG_DEBUG, 'Singularity interactive approach.')
        elif self.e.progname.find("singularity") != -1:
            self.container_type = "service"
            pbs.logmsg(pbs.LOG_DEBUG, 'Singularity service approach.')
        elif self.e.progname.find("bash") != -1:
            self.container_type = "script"
            pbs.logmsg(pbs.LOG_DEBUG, 'Singularity script approach.')
        else:
            self.container_type = "executable"
            pbs.logmsg(pbs.LOG_DEBUG, 'Singularity executable approach.')

        self.check_job_file()
        self.launch_job()



    def __execjob_end_handler(self):
        pbs.logmsg(pbs.LOG_DEBUG, 'Singularity execjob_end handler start')

        if self.check_container_running():
            self.destroy_container()



    def main(self):
        if self.hook_events[self.e.type]:
            self.hook_events[self.e.type]()



    def accept(self):
        self.e.accept()



    def call_as_user(self, call):
        user_name = self.j.Job_Owner.split("@")[0]
        krb_ticket=f'export KRB5CCNAME=/tmp/krb5cc_pbsjob_{self.jid}; cd;'
        return ["su", user_name, "-c", f'{krb_ticket} {" ".join(call)}']



    def get_singularity_vars(self):
        self.image = None
        vars = str(self.j.Variable_List).split(",")
        for var in vars:
            if var.startswith("PBS_SINGULARITY_IMAGE="):
                name = var.split("=", 1)
                self.image = name[1]
                break
                
        pbs.logmsg(pbs.LOG_DEBUG, f'Singularity image is: {self.image}')



    def check_job_file(self):
        job_file = pbs.pbs_conf["PBS_HOME"] + f'/mom_priv/jobs/{self.jid}.SC'
        if os.path.isfile(job_file):
            self.job_file = job_file



    def check_container_running(self):
        self.is_running = False

        call = ['singularity', 'instance', 'list', '2>/dev/null']
        call = self.call_as_user(call)

        grep = ['grep', self.jid]

        l = subprocess.Popen(call, stdout=subprocess.PIPE)
        g = subprocess.Popen(grep, stdout=subprocess.PIPE, stdin=l.stdout)
        stdout, stderr = g.communicate()

        if len(stdout) > 0:
            self.is_running = True
            if not self.container_type:
                self.container_type = "service"
            pbs.logmsg(pbs.LOG_DEBUG, f'Singularity container {self.jid} has been found.')

        return self.is_running



    def create_container(self):
        if self.is_running:
            pbs.logmsg(pbs.LOG_DEBUG, f'Singularity container {self.jid} already running!')
            return

        pbs.logmsg(pbs.LOG_DEBUG, f'Singularity image is: {self.image}, creating...')
        
        call = ["singularity", "instance", "start"]
        call += ['--bind', '/var/spool/pbs']
        call += ['--bind', '/etc/pbs.conf']
        call.append(self.image)
        call.append(self.jid)

        pbs.logmsg(pbs.LOG_DEBUG, f'Singularity create call is: {str(call)}')
        result = subprocess.run(self.call_as_user(call), capture_output = True, text = True)
        pbs.logmsg(pbs.LOG_DEBUG, f'Singularity call stderr is: {result.stderr}')



    def destroy_container(self):
        call = ["singularity", "instance", "stop"]
        call.append(self.jid)

        pbs.logmsg(pbs.LOG_DEBUG, f'Singularity stop call is: {str(call)}')

        result = subprocess.run(self.call_as_user(call), capture_output = True, text = True)
        pbs.logmsg(pbs.LOG_DEBUG, f'Singularity call stderr is: {result.stderr}')


    def launch_job(self):
        args = copy.deepcopy(self.e.argv)

        self.e.progname = "/usr/bin/singularity"
        self.e.argv = []
        self.e.argv.append("singularity")

        if self.container_type == "interactive":
            self.e.argv.append("shell")
            self.e.argv.append(f'instance://{self.jid}')
            return

        if self.container_type == "script":
            self.e.argv.append("shell")
            self.e.argv.append(f'instance://{self.jid}')
            self.e.argv.append("/bin/bash")
            if not self.job_file:
                pbs.logmsg(pbs.LOG_DEBUG, f'Job file {self.job_file} is missing.')  
                return
            self.e.argv.append(self.job_file)
            return

        if self.container_type == "service":
            self.e.argv.append("shell")
            self.e.argv.append(f'instance://{self.jid}')
            return

        if self.container_type == "executable":
            self.e.argv.append("exec")
            self.e.argv.append(f'instance://{self.jid}')
            self.e.argv.append("/bin/bash")
            self.e.argv.append("-c")
            executable = ""
            for arg in args:
                executable += arg + " "
            self.e.argv.append(executable)
            return



try:
    e = pbs.event()
    singularity = Singularity(e)
    singularity.get_singularity_vars()
    if not singularity.image:
        singularity.accept()
    singularity.main()
    singularity.accept()
                
except SystemExit:
    pass
except Exception as err:
    e.reject(f'Singularity hook failed: {str(err)}')

