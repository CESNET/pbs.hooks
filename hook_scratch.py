import pbs
import re
import os
import pwd
import grp
import json
import socket
import time
import errno
import shutil

job_deadsize_refresh = 86400 # 86400 = 1 day
nonjob_deadsize_refresh = 7200 # 7200 = 2 hours
dead_size_filename = ".dead.size"
scratch_paths = {"scratch_local":"scratch", "scratch_ssd":"scratch.ssd", "scratch_shared": "scratch.shared", "scratch_shm": "scratch.shm"}
scratch_types = {"scratch_local":"local", "scratch_ssd":"ssd", "scratch_shared": "shared", "scratch_shm": "shm"}
scratch_shm_dir = "/dev/shm"



def parse_cfg():
    config = {}
    if 'PBS_HOOK_CONFIG_FILE' in os.environ:
        config_file = os.environ["PBS_HOOK_CONFIG_FILE"]
        try:
            config = json.loads(open(config_file, 'r').read())
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook; failed to open config file %s: %s" % (config_file, str(err)))
            config = {}
            return config

    for i in config.keys():
        if not i in scratch_types.keys():
            pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook; failed to parse config file, incorrect scratch type %s" % str(i))
            config = {}
            return config

        for j in config[i].keys():
            if j == "disabled":
                continue

            if not j in scratch_types.keys():
                pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook; failed to parse config file, incorrect scratch type %s" % str(j))

            if not list == type(config[i][j]):
                pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook; failed to parse config file, incorrect nodes type")
                config = {}
                return config

    return config

def parse_size_resource(res, input):
    kb = 0
    m = re.search(res + '=([0-9]+)([a-zA-Z]+)', input)
    if m:
        size = int(m.group(1))
        unit = m.group(2).lower()
        if unit == "b":
            kb = int(size / 1024)
        if unit == "kb":
            kb = size
        if unit == "mb":
            kb = int(size * 1024)
        if unit == "gb":
            kb = int(size * 1024 * 1024)
        if unit == "tb":
            kb = int(size * 1024 * 1024 * 1024)
    return kb

def parse_exec_vnode(exec_vnode, schedselect):
    resources = {}
    for i in str(exec_vnode).split("+"):
        i = i.replace("(","")
        i = i.replace(")","")

        node_i = i.split(":")[0].split(".")[0]

        if not node_i in resources.keys():
            resources[node_i] = {}

        # posledni nalezeny scratch je pouzity
        for scratch_i in ["scratch_shared", "scratch_ssd", "scratch_local"]:
            size = parse_size_resource(scratch_i, i)
            if size:
                if not scratch_i in resources[node_i].keys():
                    resources[node_i][scratch_i] = 0
                resources[node_i][scratch_i] += size
                resources[node_i]["scratch_type"] = scratch_i

        if schedselect:
            m = re.search('scratch_shm=[tT]rue', str(schedselect))
            if m:
                resources[node_i]["scratch_type"] = "scratch_shm"
                resources[node_i]["scratch_shm"] = 409600
                size = parse_size_resource("mem", i)
                if size:
                    resources[node_i]["scratch_shm"] = size

    return resources

def check_scratch_use_as(config, scratch_type, node):
    # check if type conversion is defined
    if scratch_type and scratch_type in config.keys():
        scratch_use_as = None
        for scratch_as in config[scratch_type].keys():
            if node in config[scratch_type][scratch_as]:
                scratch_use_as = str(scratch_as)
                resources[node][scratch_use_as] = resources[node][scratch_type]

        return scratch_use_as

    return None

def set_permissions(user, umask, path):
    uid = pwd.getpwnam(user).pw_uid
    gid = pwd.getpwnam(user).pw_gid
    if group:
        # override group setup
        try:
            # list all user suplemental groups - for cross-check
            groups = [g.gr_name for g in grp.getgrall() if user in g.gr_mem]
            # add user primary group - gid is still user primary group id
            groups.append(grp.getgrgid(gid).gr_name)
            if group in groups:
                # try to use provided group name
                gid = grp.getgrnam(group).gr_gid
        except:
            # this should not happen but ignore if the group is not defined on this node
            pass
    os.chown(path, uid, gid)
    if umask:
        # mode is a complement to umask
        os.chmod(path,0o777^int(str(umask),8))



try:
    e = pbs.event()



    if e.type in [pbs.QUEUEJOB, pbs.MODIFYJOB]:
        j = e.job
        if "scratch_shared" in j.Resource_List.keys():
            e.reject("scratch_shared requires 'select' syntax")

        scratch_shared = False
        if "select" in j.Resource_List.keys():
            for i in str(j.Resource_List["select"]).split("+"):
                m = re.search('.*scratch_shared.*', i)
                if m:
                    scratch_shared = True
        if scratch_shared:
            if "place" in j.Resource_List.keys():
                m = re.search('.*group=.*', str(j.Resource_List["place"]))
                if not m:
                    j.Resource_List["place"] = pbs.place(str(j.Resource_List["place"]) + ":group=cluster")
            else:
                j.Resource_List["place"] = pbs.place("group=cluster")

        if "select" in j.Resource_List.keys():
            m = re.search('.*scratch_shm=[Tt]rue.*', str(j.Resource_List["select"]))
            if m:
                for scratch_i in scratch_types.keys():
                    if scratch_i == "scratch_shm":
                        continue

                    if scratch_i in str(j.Resource_List["select"]):
                        e.reject("can not combine %s and scratch_shm" % scratch_i)



    if e.type == pbs.EXECJOB_BEGIN:
        j = e.job
        scratch_type = None
        include_host_dir = False
        node = pbs.get_local_nodename()

        config = parse_cfg()

        pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, node: %s" % node)
        pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, %s has exec_vnode: %s" % (j.id, str(j.exec_vnode)))
        pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, %s has schedselect: %s" % (j.id, str(j.schedselect)))
        
        resources = parse_exec_vnode(j.exec_vnode, j.schedselect)

        pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, %s scratch resources: %s" % (j.id, str(resources)))

        # pokud byl pro node zadan typ scratche, nastavime ho do scratch_type
        if node in resources.keys() and "scratch_type" in resources[node].keys():
            scratch_type = resources[node]["scratch_type"]

        # check if type conversion is defined
        scratch_use_as = check_scratch_use_as(config, scratch_type, node)

        if scratch_type == "scratch_local" and scratch_use_as == "scratch_shared":
            include_host_dir = True

        if scratch_use_as:
            scratch_type = scratch_use_as

        if scratch_type:
            scratch_size = resources[node][scratch_type]

            pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, %s: %s %s: %d" % (j.id, node, scratch_type, scratch_size))

            if scratch_size == 0:
                pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, 0kb scratch requested, scratch hook stopped")
                e.accept()

            # vytvoreni adresare - fixed by Petr Kulhanek
            user = j.Job_Owner.split("@")[0]
            # FIXME
            # group is taken from the job if set
            # this implementation consider only the first group name and ignore domain name
            group = j.group_list
            if group:
                group = str(j.group_list).split("@")[0]
            # umask is taken from the job if set
            umask = j.umask

            if include_host_dir:
                path="/%s/%s/%s/job_%s" % (scratch_paths[scratch_type], socket.gethostname(), user, j.id)
            else:
                path="/%s/%s/job_%s" % (scratch_paths[scratch_type], user, j.id)

            if scratch_type == "scratch_shm":
                path = scratch_shm_dir + path

            if os.path.isdir(path):
                exists_prefix = ".run_count"
                try:
                    backup_path = "%s/%s-%d" % (path, exists_prefix, j.run_count)
                    os.mkdir(backup_path)
                    set_permissions(user, umask, backup_path)

                    files = os.listdir(path)
                    for file in files:
                        if file.startswith(exists_prefix):
                            continue
                        shutil.move(os.path.join(path, file), backup_path)
                except Exception as err:
                    pbs.logmsg(pbs.EVENT_DEBUG, "Failed to create and move to %s-%d SCRATCHDIR %s, err: %s" % (exists_prefix, j.run_count, path, str(err)))
                    e.reject("scratch hook failed: %s" % str(err))
            else:
                try:
                    if scratch_type == "scratch_shm" or include_host_dir:
                        os.makedirs(path)
                    else:
                        os.mkdir(path)
                    set_permissions(user, umask, path)

                except Exception as err:
                    pbs.logmsg(pbs.EVENT_DEBUG, "Failed to create SCRATCHDIR %s, err: %s" % (path, str(err)))
                    e.reject("scratch hook failed: %s" % str(err))

            #nastaveni env, hodnoty vynasobime 1024, jednotky maji byt v B

            j.Variable_List["SCRATCHDIR"]=path
            j.Variable_List["SCRATCH"]=path
            j.Variable_List["SINGULARITY_TMPDIR"]=path
            j.Variable_List["SINGULARITY_CACHEDIR"]=path

            j.Variable_List["SCRATCH_VOLUME"]=scratch_size * 1024
            j.Variable_List["PBS_RESC_SCRATCH_VOLUME"]=scratch_size * 1024
            j.Variable_List["TORQUE_RESC_SCRATCH_VOLUME"]=scratch_size * 1024

            j.Variable_List["SCRATCH_TYPE"]=scratch_types[scratch_type]

            if scratch_type == "scratch_local":
                j.Variable_List["PBS_RESC_SCRATCH_LOCAL"]=scratch_size * 1024
                j.Variable_List["TORQUE_RESC_SCRATCH_LOCAL"]=scratch_size * 1024

            if scratch_type == "scratch_shared":
                j.Variable_List["PBS_RESC_SCRATCH_SHARED"]=scratch_size * 1024
                j.Variable_List["TORQUE_RESC_SCRATCH_SHARED"]=scratch_size * 1024

            if scratch_type == "scratch_ssd":
                j.Variable_List["PBS_RESC_SCRATCH_SSD"]=scratch_size * 1024
                j.Variable_List["TORQUE_RESC_SCRATCH_SSD"]=scratch_size * 1024

            scratch_total_size = 0
            for node_i in resources.keys():
                if scratch_type in resources[node_i].keys():
                    scratch_total_size += resources[node_i][scratch_type]
            j.Variable_List["PBS_RESC_TOTAL_SCRATCH_VOLUME"]=scratch_total_size * 1024
            j.Variable_List["TORQUE_RESC_TOTAL_SCRATCH_VOLUME"]=scratch_total_size * 1024
        else:
            path="/var/tmp/pbs.%s" % j.id

            j.Variable_List["SCRATCHDIR"]=path
            j.Variable_List["SCRATCH"]=path
            j.Variable_List["SINGULARITY_TMPDIR"]=path
            j.Variable_List["SINGULARITY_CACHEDIR"]=path

            j.Variable_List["SCRATCH_VOLUME"]=0
            j.Variable_List["PBS_RESC_SCRATCH_VOLUME"]=0
            j.Variable_List["TORQUE_RESC_SCRATCH_VOLUME"]=0

            j.Variable_List["SCRATCH_TYPE"]="none"

            j.Variable_List["PBS_RESC_TOTAL_SCRATCH_VOLUME"]=0
            j.Variable_List["TORQUE_RESC_TOTAL_SCRATCH_VOLUME"]=0



    if e.type == pbs.EXECJOB_END:
        scratch_type = None
        include_host_dir = False
        j = e.job
        node = pbs.get_local_nodename()
        user = j.Job_Owner.split("@")[0]
        resources = parse_exec_vnode(j.exec_vnode, j.schedselect)

        config = parse_cfg()

        # pokud byl pro node zadan typ scratche, nastavime ho do scratch_type
        if node in resources.keys() and "scratch_type" in resources[node].keys():
            scratch_type = resources[node]["scratch_type"]

        # check if type conversion is defined
        scratch_use_as = check_scratch_use_as(config, scratch_type, node)

        if scratch_type == "scratch_local" and scratch_use_as == "scratch_shared":
            include_host_dir = True

        if scratch_use_as:
            scratch_type = scratch_use_as

        if scratch_type:
            if include_host_dir:
                path="/%s/%s/%s/job_%s" % (scratch_paths[scratch_type], socket.gethostname(), user, j.id)
            else:
                path="/%s/%s/job_%s" % (scratch_paths[scratch_type], user, j.id)

            if scratch_type == "scratch_shm":
                path = scratch_shm_dir + path
                try:
                    shutil.rmtree(path)
                except:
                    pbs.logmsg(pbs.EVENT_DEBUG, "%s;scratch shm: %s failed to clear" % (j.id, path))
            else:
                try:
                    os.rmdir(path)
                    pbs.logmsg(pbs.EVENT_DEBUG, "%s;Empty scratch: %s removed" % (j.id, path))
                except OSError as ex:
                    if ex.errno == errno.ENOTEMPTY:
                        pbs.logmsg(pbs.EVENT_DEBUG, "%s;scratch: %s not empty" % (j.id, path))



    if e.type == pbs.EXECHOST_PERIODIC:
        dyn_res = {}



        # check if pid is running
        def check_pid(pid):
            try:
                os.kill(pid, 0)
            except OSError:
                return False
            return True



        # read deadsize from file
        def read_deadsize(f_dead_size):
            # read deadsize from file or return (0,0)
            dead_size = 0
            mtime = 0

            if os.path.isfile(f_dead_size):
                # try to read the dead size from file - if exists
                with open(f_dead_size, 'r') as f:
                    try:
                        dead_size = int(f.read())
                    except:
                        dead_size = 0

                mtime = os.stat(f_dead_size).st_mtime

            return (dead_size, mtime)



        # write pidfile and nodefile
        def write_pid(pid, pidfile, nodefile):
            try:
                this_node = socket.gethostname()
            except:
                this_node = "none"

            # this is the parent, save the pid file
            try:
                with open(pidfile, 'w') as f:
                    f.write(str(pid))
            except:
                pbs.logmsg(pbs.EVENT_ERROR, "scratch hook failed to write pidfile %s" % pidfile)

            try:
                with open(nodefile, 'w') as f:
                    f.write(str(this_node))
            except:
                pbs.logmsg(pbs.EVENT_ERROR, "scratch hook failed to write file %s" % nodefile)

        def write_deadsize(f_dead_size, dead_size):
            try:
                with open(f_dead_size, 'w') as f:
                    f.write(str(dead_size))
            except:
                pass



        ################################################################
        # check if it is safe to fork the process
        # no other process is already running or other node is checking
        # the job scratch
        ################################################################
        def is_ok_to_fork(jobpath, nodefile, pidfile):
            try:
                this_node = socket.gethostname()
            except:
                this_node = "none"

            checking_node = "none"
            if os.path.isfile(nodefile):
                try:
                    with open(nodefile, 'r') as f:
                        checking_node = f.read().strip()
                except:
                    checking_node = "none"

            if os.path.isfile(pidfile):
                # is the check already runnning?
                try:
                    with open(pidfile, 'r') as f:
                        pid = int(f.read())
                except:
                    pid = 0

                if pid and this_node != checking_node and checking_node != 'none':
                    # different node is checking, not finished yet
                    pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook node %s still checking %s" % (checking_node, jobpath))
                    return False

                if pid and check_pid(pid):
                    # not finished yet
                    pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook pid %d still checking %s" % (pid, jobpath))
                    return False

            return True



        ################################################################
        # get the size of the job scratch directory for a particular job
        # the process forks and the parent reports the value
        # first time zero is reported
        ################################################################
        def get_deadsize(jobpath):
            f_dead_size = os.path.join(jobpath, dead_size_filename)
            f_dead_size_pidfile = os.path.join(jobpath, dead_size_filename + ".pid")
            f_dead_size_nodefile = os.path.join(jobpath, dead_size_filename + ".node")

            (dead_size, mtime) = read_deadsize(f_dead_size)

            if not dead_size or int(time.time()) - mtime > job_deadsize_refresh:

                if not is_ok_to_fork(jobpath, f_dead_size_nodefile, f_dead_size_pidfile):
                    return dead_size

                #start the check of dead size for a particular job
                pid = os.fork()
                if pid == 0:
                    # the child
                    for path, dirs, files in os.walk(jobpath):
                        for f in files:
                            filepath = os.path.join(path, f)

                            if os.path.islink(filepath):
                                continue

                            if not os.path.isfile(filepath):
                                continue

                            s = os.stat(filepath)
                            if s.st_blocks:
                                dead_size += (s.st_blocks * 512) # in B

                    dead_size = dead_size/1024 # in KB

                    write_deadsize(f_dead_size, dead_size)

                    # we need thorough exit, otherwise, the pbs_python will continue to run
                    os._exit(0)

                if pid > 0:
                    # this is the parent, save the pid file
                    write_pid(pid, f_dead_size_pidfile, f_dead_size_nodefile)

            #pbs.logmsg(pbs.EVENT_DEBUG, "SCRATCH path %s size %s" % (jobpath, str(dead_size)))

            # return the dead size for finished check or zero for first time check
            return dead_size



        ################################################################
        # get the size of deadspace outside of job dirs
        # this means size of files in user directories and in the
        # root directory of this scratch
        ################################################################
        def get_nonjob_trash(scratchpath):
            f_dead_size = os.path.join(scratchpath, dead_size_filename)
            f_dead_size_pidfile = os.path.join(scratchpath, dead_size_filename + ".pid")
            f_dead_size_nodefile = os.path.join(scratchpath, dead_size_filename + ".node")

            (dead_size, mtime) = read_deadsize(f_dead_size)

            if not dead_size or int(time.time()) - mtime > nonjob_deadsize_refresh:

                if not is_ok_to_fork(scratchpath, f_dead_size_nodefile, f_dead_size_pidfile):
                    return dead_size

                # start the check of dead size for nonjob trash
                pid = os.fork()
                if pid == 0:
                    # the child
                    filestocheck = []

                    # listdir the root of scratch
                    for i in os.listdir(scratchpath):
                        p = os.path.join(scratchpath, i)

                        if os.path.islink(p):
                            continue

                        # check files in scratch root
                        if os.path.isfile(p):
                            filestocheck.append(p)

                        if not os.path.isdir(p):
                            continue

                        # check files in user dirs and ouside of job dirs
                        for j in os.listdir(p):
                            k = os.path.join(p, j)

                            if os.path.islink(k):
                                continue

                            if os.path.isfile(k):
                                filestocheck.append(k)

                    for i in filestocheck:
                        if os.path.islink(i):
                            continue

                        if not os.path.isfile(i):
                            continue

                        s = os.stat(i)
                        if s.st_blocks:
                            dead_size += (s.st_blocks * 512) # in B

                    dead_size = dead_size/1024 # in KB

                    write_deadsize(f_dead_size, dead_size)

                    # we need thorough exit, otherwise, the pbs_python will continue to run
                    os._exit(0)

                if pid > 0:
                    # this is the parent, save the pid file
                    write_pid(pid, f_dead_size_pidfile, f_dead_size_nodefile)

            return dead_size



        ################################################################
        # get the available size of a particular scratch partition
        # take the total size and subtract the deadspace (both jobs and other trash)
        # do not consider running jobs in this scratch, pbs will do it
        ################################################################
        def get_avail(scratch_type, running_jobs):
			# first, we get the total size and then we subtract the dead size from total size

            global scratch_paths

            total_size = 0
            total_dead_size = 0

            try:
				# the total size of the scratch partition
                s = os.statvfs("/%s/" % scratch_paths[scratch_type])
                total_size = (s.f_bsize * s.f_blocks) /1024
            except:
                return 0

            try:
                s = os.statvfs("/%s/" % scratch_paths[scratch_type])
                free_size = (s.f_bsize * s.f_bavail) /1024
            except:
                free_size = 0

            # for each user directory and each job directory in the scratch dir
            # check the deadsize but only if the job is not runnning
            for user in os.listdir(os.path.join("/", scratch_paths[scratch_type])):
                user_path = os.path.join("/", scratch_paths[scratch_type], user)

                if not os.path.isdir(user_path):
                    continue

                for job in os.listdir(user_path):
                    job_path = os.path.join("/", scratch_paths[scratch_type], user, job)

                    if not os.path.isdir(job_path):
                        continue

                    if job.replace("job_", "") in running_jobs:
                        # do not count running jobs
                        continue

                    total_dead_size += get_deadsize(job_path)

            # check files outside job directories
            total_dead_size += get_nonjob_trash(os.path.join("/", scratch_paths[scratch_type]))

            workspace = total_size - total_dead_size

            # reserved = SUM(reserved[j], j in running_jobs)
            reserved = 0
            for job in pbs.event().job_list.keys():
                local_node = pbs.get_local_nodename()
                resources = parse_exec_vnode(pbs.event().job_list[job].exec_vnode, None)
                if local_node in resources.keys() and "scratch_type" in resources[local_node].keys() and scratch_type == resources[local_node]["scratch_type"]:
                    reserved = resources[local_node][scratch_type]

            free_correction = max(0, workspace - reserved - free_size)

            pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook %s total_dead_size: %d workspace: %d reserved: %d free_correction %d free_size %s" % (scratch_type, total_dead_size, workspace, reserved, free_correction, free_size))

            return max(0, workspace - free_correction)



        vnl = pbs.event().vnode_list
        local_node = pbs.get_local_nodename()

        config = parse_cfg()

        for scratch_i in scratch_types.keys():
            # get scratch size for each scratch type

            scratch_use_as = scratch_i

            if scratch_i in config.keys():
                for scratch_as in config[scratch_i]:
                    if scratch_as == "disabled":
                        continue

                    if local_node in config[scratch_i][scratch_as]:
                        pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, using %s as %s" % (scratch_as, scratch_i))
                        scratch_use_as = scratch_as

            # scratch shm is only boolean, no size
            if (scratch_i == "scratch_shm"):
                if os.path.isdir(scratch_shm_dir):
                    vnl[local_node].resources_available[scratch_i] = True
                else:
                    vnl[local_node].resources_available[scratch_i] = False
                continue

            # get the scratch size without dead space
            dyn_res[scratch_i] = get_avail(scratch_use_as, pbs.event().job_list.keys())

            # if there is a scratch "used as", we need to subtract running jobs of the other scratch

            # first, which type of scratch should by subtracted
            subtract_scratch = None
            for scratch_c in config.keys():
                for scratch_as in config[scratch_c]:
                    if scratch_i == scratch_c:
                        if local_node in config[scratch_c][scratch_as]:
                            subtract_scratch = scratch_as
                    else:
                        if scratch_i == scratch_as and local_node in config[scratch_c][scratch_as]:
                            subtract_scratch = scratch_c

            # second, the actual subtract
            if subtract_scratch:
                for job in pbs.event().job_list.keys():
                    resources = parse_exec_vnode(pbs.event().job_list[job].exec_vnode, None)
                    if local_node in resources.keys() and "scratch_type" in resources[local_node].keys() and subtract_scratch == resources[local_node]["scratch_type"]:
                        dyn_res[scratch_i] -= resources[local_node][subtract_scratch]

            if scratch_i in config.keys():
                if "disabled" in config[scratch_i]:
                    if local_node in config[scratch_i]["disabled"]:
                        dyn_res[scratch_i] = 0

            vnl[local_node].resources_available[scratch_i] = pbs.size("%dkb" % int(dyn_res[scratch_i]))
            pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, reporting %s %s %dkb" % (local_node, scratch_i, int(dyn_res[scratch_i])))

        pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, reported %s" % str(dyn_res))

except Exception as err:
    e.reject("scratch hook failed: %s" % str(err))
