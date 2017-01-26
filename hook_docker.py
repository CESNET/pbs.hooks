import pbs
import os
import copy
import re



class Docker(object):
    e = None
    j = None
    jid = None
    image = None
    entrypoint = None
    is_running = False
    job_file = None
    container_type = None #interactive, service, script, executable
    hook_events = {}

    ignored_env = ["PATH", "TMPDIR"]



    def __init__(self, pbs_event):
        self.hook_events = { pbs.QUEUEJOB: self.__queuejob_handler,
                             pbs.MODIFYJOB: self.__queuejob_handler,
                             pbs.EXECJOB_LAUNCH: self.__execjob_launch_handler,
                             pbs.EXECJOB_PROLOGUE: self.__execjob_prologue_handler,
                             pbs.EXECJOB_END: self.__execjob_end_handler,
                           }

        self.e = pbs_event
        self.j = self.e.job
        self.jid = self.j.id



    def __queuejob_handler(self):
        pbs.logmsg(pbs.LOG_DEBUG, "Docker queuejob handler start")

        newselect = []
        if "select" in self.j.Resource_List.keys():
            for i in str(self.j.Resource_List["select"]).split("+"):
                if re.search("docker=[Tt]{1}rue", i):
                    newselect.append(i)
                    continue
                newselect.append(i + ":docker=true")
        else:
            newselect.append("docker=true")

        pbs.logmsg(pbs.LOG_DEBUG, "Old select: %s" % str(self.j.Resource_List))
        self.j.Resource_List["select"] = pbs.select("+".join(newselect))
        pbs.logmsg(pbs.LOG_DEBUG, "New select: %s" % str(self.j.Resource_List))



    def __execjob_launch_handler(self):
        pbs.logmsg(pbs.LOG_DEBUG, "Docker execjob_launch handler start")

        if self.j.interactive:
            self.container_type = "interactive"
            pbs.logmsg(pbs.LOG_DEBUG, "It's an interactive")
        elif self.e.progname.find("docker") != -1:
            self.container_type = "service"
            pbs.logmsg(pbs.LOG_DEBUG, "It's a service")
        elif self.e.progname.find("bash") != -1:
            self.container_type = "script"
            pbs.logmsg(pbs.LOG_DEBUG, "It's a script")
        else:
            #musi byt posledni
            self.container_type = "executable"
            pbs.logmsg(pbs.LOG_DEBUG, "It's an executable")             

        self.check_container_running()
        self.check_job_file()
        self.create_container()
        self.launch_job()



    def __execjob_prologue_handler(self):
        # true pokud primarni exec. host
        if self.j.in_ms_mom():
            self.accept()

        pbs.logmsg(pbs.LOG_DEBUG, "Docker execjob_prologue handler start")

        self.container_type = "service"
        self.check_container_running()
        self.check_job_file()
        self.create_container()



    def __execjob_end_handler(self):
        pbs.logmsg(pbs.LOG_DEBUG, "Docker execjob_end handler start")

        call = "docker stop " + str(self.jid)
        pbs.logmsg(pbs.LOG_DEBUG, "Call is: %s" % call)
        os.system(call)

        call = "docker rm " + str(self.jid)
        pbs.logmsg(pbs.LOG_DEBUG, "Call is: %s" % call)
        os.system(call)



    def main(self):
        self.hook_events[self.e.type]()



    def accept(self):
        self.e.accept()



    def get_docker_vars(self):
        self.image = None
        self.entrypoint = None
        vars = str(self.j.Variable_List).split(",")
        for var in vars:
            #pbs.logmsg(pbs.LOG_DEBUG, "Variable is: %s" % var )
            if var.startswith("DOCKER_IMAGE="):
                name = var.split("=", 1)
                self.image = name[1]
                
            if var.startswith("DOCKER_ENTRYPOINT="):
                name = var.split("=", 1)
                self.entrypoint = name[1]

        pbs.logmsg(pbs.LOG_DEBUG, "Docker image is: %s" % self.image)



    def check_container_running(self):
        self.is_running = False
        search_container = "docker ps 2>/dev/null | grep %s | grep -v grep" % str(self.jid)
        try:
            already_running = os.popen(search_container).read()
        except:
            self.is_running = False

        if already_running:
            self.is_running = True
            if not self.container_type:
                self.container_type = "service"
            pbs.logmsg(pbs.LOG_DEBUG, "Docker container found!")

        return self.is_running



    def add_resource_restriction(self, call):
        n = self.j.Resource_List["ncpus"]
        m = self.j.Resource_List["mem"]
        if n:
            #call += " -c %s" % str(n)
            call += " -c 0"
        if m:
            call += " -m %s" % str(m)
        return call



    def add_env(self, call):
        # v env se me obcas vyskytovala chybna data, odstranit
        for i in self.e.env.keys():
            if len(self.e.env[i]) == 0:
                del self.e.env[i]

        for i in self.e.env.keys():
            if str(i) in self.ignored_env:
                continue
            call += " -e %s=%s" % (str(i), self.e.env[i])
        return call



    def check_job_file(self):
        job_file = pbs.pbs_conf["PBS_HOME"] + "/mom_priv/jobs/" + str(self.jid) + ".SC"
        if os.path.isfile(job_file):
            self.job_file = job_file



    def create_container(self):
        if self.is_running:
            pbs.logmsg(pbs.LOG_DEBUG, "Docker container %s already running!")
            return

        pbs.logmsg(pbs.LOG_DEBUG, "Docker image is: %s, creating..." % self.image)

        call = "docker run -d -it --name %s" % str(self.jid)

        #if self.container_type == "interactive":
        #    call += " -it"

        call = self.add_resource_restriction(call)
        pbs.logmsg(pbs.LOG_DEBUG, "Resource is: %s" % call)

        call = self.add_env(call)
        pbs.logmsg(pbs.LOG_DEBUG, "Env is: %s" % call)
        
        if self.job_file:
            call += " -v " + self.job_file + ":" + self.job_file 
            pbs.logmsg(pbs.LOG_DEBUG, "Job file is: %s" % self.job_file)  
            
        if self.entrypoint:
            call += " --entrypoint " + self.entrypoint
			
        user_name = self.j.Job_Owner.split("@")[0]
        call += " -v /home/" + user_name + ":/home/" + user_name + ":ro"

        #call += " --net=host"

        call += " " + self.image

        pbs.logmsg(pbs.LOG_DEBUG, "Call is: %s" % call)
        os.popen(call)



    def launch_job(self):
        args = copy.deepcopy(self.e.argv)
        pbs.logmsg(pbs.LOG_DEBUG, "args are %s" % self.e.progname)

        self.e.progname = "/usr/bin/docker"
        self.e.argv = []
        self.e.argv.append("docker")
        self.e.argv.append("exec")
        if self.container_type == "interactive":
            self.e.argv.append("-it")
        self.e.argv.append(self.jid)

        if self.container_type == "interactive":
            self.e.argv.append("/bin/bash")
            return

        if self.container_type == "script":
            self.e.argv.append("/bin/bash")
            if not self.job_file:
                pbs.logmsg(pbs.LOG_DEBUG, "Job file is missing")  
                return
            self.e.argv.append("-c")
            self.e.argv.append(self.job_file)
            return

        if self.container_type == "service":
            self.e.argv.append("/bin/sh")
            return

        if self.container_type == "executable":
            self.e.argv.append("/bin/bash")
            self.e.argv.append("-c")
            executable = ""
            for arg in args:
                executable += arg + " "
                pbs.logmsg(pbs.LOG_DEBUG,"arg: %s" % arg)
            self.e.argv.append(executable)
            return



try:
    e = pbs.event()
    docker = Docker(e)    
    docker.get_docker_vars()
    if not docker.image:
        docker.accept()

    docker.main()
    docker.accept()
                
except SystemExit:
    pass
except Exception as err:
    e.reject("docker hook failed: %s " % str(err))

