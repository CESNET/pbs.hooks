import pbs
import os
import subprocess
import re

'''
create resource gpupercent
set resource gpupercent type = long
set resource gpupercent flag = r

create resource gpumemmaxpercent
set resource gpumemmaxpercent type = long
set resource gpumemmaxpercent flag = r

create resource gpupowerusageavg
set resource gpupowerusageavg type = float
set resource gpupowerusageavg flag = r
'''

DCGMI_GROUPID_LOCATION="/tmp"
DCGMI_GROUPID_PREFIX="pbs_dcgmi_groupid"

def parse_exec_vnode(exec_vnode):
    resources = {}
    for i in str(exec_vnode).split("+"):
        i = i.replace("(","")
        i = i.replace(")","")

        node_i = i.split(":")[0].split(".")[0]

        if not node_i in resources.keys():
            resources[node_i] = {}

        m = re.search('ngpus=([0-9]+?)', i)
        if m:
            if not 'ngpus' in resources[node_i].keys():
                resources[node_i]['ngpus'] = 0
            resources[node_i]['ngpus'] += int(m.group(1))

    return resources

def check_is_nvidia():
    return os.path.isfile('/usr/bin/nvidia-smi') and os.path.isfile('/usr/bin/dcgmi')

def check_dcgmi_started(jobid):
    return os.path.isfile(f'{DCGMI_GROUPID_LOCATION}/{DCGMI_GROUPID_PREFIX}_{jobid}')

def get_resource_ngpus(resources):
    local_node = pbs.get_local_nodename()
    
    if local_node in resources and 'ngpus' in resources[local_node]:
        return resources[local_node]['ngpus']
    return 0

def create_dcgmi_group(jobid):
    groupid = -1
    cmd = ["dcgmi", "group", "-c", jobid]

    out = []
    try:
        process = subprocess.Popen(cmd, shell=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
        out = process.communicate()[0].split('\n')
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to create dcgmi group: {str(err)}")
        return groupid

    m = re.search('with a group ID of ([0-9]+?)$', out[0])
    if m:
        groupid = int(m.group(1))
    
    return groupid

def add_to_dcgmi_group(groupid, id):
    cmd = ["dcgmi", "group", "-g", str(groupid), '-a', str(id)]
    try:
        process = subprocess.Popen(cmd, shell=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
        process.communicate()
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to add uuid to dcgmi group: {str(err)}")
        
def enable_dcmgi_stats(groupid, jobid):
    cmd = ["dcgmi", "stats", "-e"]
    try:
        process = subprocess.Popen(cmd, shell=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
        process.communicate()
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to add enable dcgmi stats: {str(err)}")

    cmd = ["dcgmi", "stats", "-g", str(groupid), "-s", jobid]
    try:
        process = subprocess.Popen(cmd, shell=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
        process.communicate()
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to add start dcgmi stats: {str(err)}")

def disable_dcmgi_stats(jobid):
    cmd = ["dcgmi", "stats", "-x", jobid]
    try:
        process = subprocess.Popen(cmd, shell=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
        process.communicate()
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to add disable dcgmi stats: {str(err)}")

    cmd = ["dcgmi", "stats", "-r", jobid]
    try:
        process = subprocess.Popen(cmd, shell=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
        process.communicate()
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to add delete dcgmi stats: {str(err)}")

def delete_dcgmi_group(groupid):
    cmd = ["dcgmi", "group", "-d", str(groupid)]
    try:
        process = subprocess.Popen(cmd, shell=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
        process.communicate()
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to delete dcgmi group: {str(err)}")

def write_groupid(jobid, groupid):
    try:
        f = open (f'{DCGMI_GROUPID_LOCATION}/{DCGMI_GROUPID_PREFIX}_{jobid}','w')
        f.write("%s" % groupid)
        f.close()
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to write dcgmi groupid: {str(err)}")

def read_and_delete_groupid(jobid):
    try:
        f = open (f'{DCGMI_GROUPID_LOCATION}/{DCGMI_GROUPID_PREFIX}_{jobid}','r')
        groupid = int(f.readline())
        f.close()

        os.remove(f'{DCGMI_GROUPID_LOCATION}/{DCGMI_GROUPID_PREFIX}_{jobid}')
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to read dcgmi groupid: {str(err)}")
        return -1
    return groupid

def get_gpu_id(uuid):
    cmd = ["dcgmi", "discovery", "-l"]

    out = []
    try:
        process = subprocess.Popen(cmd, shell=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
        out = process.communicate()[0].split('\n')
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to run nvidia-smi: {str(err)}")
        return
    
    id = -1
    for line in out:
        l = line.split("|")
        if len(l) == 4:
            if l[2].strip().startswith("Name:"):
                id =  int(l[1].strip())
            if l[2].strip() == f"Device UUID: {uuid}":
                return id
    return -1

def get_gpu_mem():
    gpu_mem = 0
    cmd = ['nvidia-smi', '--query-gpu=memory.total', '--format=csv', '-i', '0']
    try:
        process = subprocess.Popen(cmd, shell=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
        out = process.communicate()[0].split('\n')

        if process.returncode != 0:
            return gpu_mem
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to get memory. Running nvidia-smi failed: {str(err)}")
        return gpu_mem

    for line in out:
        l = line.split()
        if len(l) == 2 and l[1] == "MiB":
            try:
                gpu_mem = int(l[0]) * 1024 * 1024
            except:
                gpu_mem = 0

    return gpu_mem

def parse_dcgmi_stats(job, jobid, gpumem):
    if gpumem <= 0:
        return

    cmd = ["dcgmi", "stats", "-j", jobid, "-v"]

    out = []
    try:
        process = subprocess.Popen(cmd, shell=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
        out = process.communicate()[0].split('\n')
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to run dcgmi stats: {str(err)}")
        return

    gpupercent = 0
    gpumemmaxpercent = 0
    gpupowerusageavg = 0
    for line in out:
        l = line.split("|")

        if len(l) == 4:
            if l[1].strip().startswith("SM Utilization"):
                m = re.search('.*Avg: ([0-9]+),.*', l[2].strip())
                if m:
                    gpupercent += int(m.group(1))

            if l[1].strip().startswith("Max GPU Memory Used"):
                m = re.search('([0-9]+)', l[2].strip())
                if m:
                    gpumemmaxpercent += int(100 * (int(m.group(1))/gpumem))

            if l[1].strip().startswith("Power Usage"):
                m = re.search('.*Avg: ([\.0-9]+),.*', l[2].strip())
                if m:
                    gpupowerusageavg += float(m.group(1))

    job.resources_used['gpupercent'] = gpupercent
    job.resources_used['gpumemmaxpercent'] = gpumemmaxpercent
    job.resources_used['gpupowerusageavg'] = gpupowerusageavg
                
                
def add_gpus_to_groupid(groupid):
    cmd = ["/usr/bin/nvidia-smi", "-L"]

    out = []
    try:
        process = subprocess.Popen(cmd, shell=False,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    universal_newlines=True)
        out = process.communicate()[0].split('\n')
    except Exception as err:
        pbs.logmsg(pbs.EVENT_ERROR, f"Failed to run nvidia-smi: {str(err)}")
        return
    
    for line in out:
        m = re.search('.*UUID: ([-A-Za-z0-9]+)\).*', line)
        if m:
            uuid = m.group(1)
            id = get_gpu_id(uuid)
            add_to_dcgmi_group(groupid, id)

try:
    e = pbs.event()

    if not check_is_nvidia():
        e.accept()

    ###
    # launch hook, starts measuring
    ###
    if e.type == pbs.EXECJOB_LAUNCH:
        job = e.job
        jobid = e.job.id

        gpumem = get_gpu_mem()

        if check_dcgmi_started(jobid):
            # we are probably on sis mom and dcgmi has been already started in previous task
            parse_dcgmi_stats(job, jobid, gpumem)
            e.accept()

        resources = parse_exec_vnode(job.exec_vnode)
        ngpus = get_resource_ngpus(resources)
        if ngpus == 0:
            e.accept()
        
        groupid = create_dcgmi_group(jobid)
        if groupid == -1:
            e.accept()

        write_groupid(jobid, groupid)
        add_gpus_to_groupid(groupid)
        enable_dcmgi_stats(groupid, jobid)

        if check_dcgmi_started(jobid):
            parse_dcgmi_stats(job, jobid, gpumem)

        e.accept()

    ###
    # host periodic hook, collects data
    ###
    if e.type == pbs.EXECHOST_PERIODIC:
        gpumem = get_gpu_mem()

        for jobid in pbs.event().job_list.keys():
            if not check_dcgmi_started(jobid):
                continue

            job = pbs.event().job_list[jobid]
            parse_dcgmi_stats(job, jobid, gpumem)

        e.accept()

    ###
    # epilogue hook, final data collection and stops measuring
    ###
    if e.type == pbs.EXECJOB_EPILOGUE:
        job = e.job
        jobid = e.job.id
        
        if not check_dcgmi_started(jobid):
            e.accept()
        
        resources = parse_exec_vnode(job.exec_vnode)
        ngpus = get_resource_ngpus(resources)
        if ngpus == 0:
            e.accept()

        gpumem = get_gpu_mem()
        parse_dcgmi_stats(job, jobid, gpumem)
        
        disable_dcmgi_stats(jobid)
        groupid = read_and_delete_groupid(jobid)
        if groupid == -1:
            e.accept()
        delete_dcgmi_group(groupid)
        e.accept()

    e.accept()
        
except Exception as err:
    pbs.logmsg(pbs.EVENT_ERROR, f"Failed to run dcgm hook. Event accepted anyway. Error: {str(err)}")
    e.accept() # do not fail job if hook fails
