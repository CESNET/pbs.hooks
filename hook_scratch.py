import pbs
import sys
import re
import sqlite3
import datetime
import os
import pwd
import grp

sqlite_db="/var/spool/pbs/resources.db"
scratch_paths = {"scratch_local":"scratch", "scratch_ssd":"scratch.ssd", "scratch_shared": "scratch.shared"}
scratch_types = {"scratch_local":"local", "scratch_ssd":"ssd", "scratch_shared": "shared"}

try:
    e = pbs.event()

    if e.type == pbs.EXECJOB_BEGIN:
        j = e.job
        scratch_type = None        
        host=e.requestor_host.split(".")[0]

        pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, host: %s" % host)
        pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, %s has exec_vnode: %s" % (j.id, str(j.exec_vnode)))
        
        resources = {}
        for i in str(j.exec_vnode).split("+"):
            i = i.replace("(","")
            i = i.replace(")","")
            
            host_i = i.split(":")[0].split(".")[0]

            if not host_i in resources.keys():
                resources[host_i] = {}

            # posledni nalezeny scratch je pouzity
            for scratch_i in ["scratch_shared", "scratch_ssd", "scratch_local"]:
                m = re.search(scratch_i + '=([0-9]+?)kb', i)
                if m:
                    if not scratch_i in resources[host_i].keys():
                        resources[host_i][scratch_i] = 0
                    resources[host_i][scratch_i] += int(m.group(1))
                    resources[host_i]["scratch_type"] = scratch_i

        pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, scratch resources: %s" % str(resources))

        # pokud byl pro hosta zadan typ scratche, nastavime ho do scratch_type
        if host in resources.keys() and "scratch_type" in resources[host].keys():
            scratch_type = resources[host]["scratch_type"]
        
        if scratch_type:
            scratch_size = resources[host][scratch_type]

            pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, %s: %s %s: %d" % (j.id, host, scratch_type, scratch_size))

            #zapamatovani resourcu v tabulce sqlite
            conn = sqlite3.connect(sqlite_db)
            c = conn.cursor()

            # pokud tabulka neexistuje, tak ji vytvorim
            c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='resources'");
            if c.fetchone() == None:
                c.execute("CREATE TABLE resources (date text, job_id text, job_owner text, resource_type text, value integer)")

            #vlozim scratch do tabulky a koncim
            now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            c.execute("INSERT INTO resources VALUES ('%s', '%s', '%s', '%s', %d)" % (now, j.id, j.Job_Owner, scratch_type, scratch_size))
            conn.commit()
            conn.close()

            # vytvoreni adresare
            user = j.Job_Owner.split("@")[0]

            path="/%s/%s/job_%s" % (scratch_paths[scratch_type], user, j.id)

            try:
                os.makedirs(path)
                uid = pwd.getpwnam(user).pw_uid
                gid = grp.getgrnam("meta").gr_gid
                os.chown(path, uid, gid)
            except:
                pbs.logmsg(pbs.EVENT_DEBUG, "Failed to create SCRATCHDIR %s" % path)

            #nastaveni env, hodnoty vynasobime 1024, jednotky maji byt v B

            j.Variable_List["SCRATCHDIR"]=path
            j.Variable_List["SCRATCH"]=path

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
            for host_i in resources.keys():
                if scratch_type in resources[host_i].keys():
                    scratch_total_size += resources[host_i][scratch_type]
            j.Variable_List["PBS_RESC_TOTAL_SCRATCH_VOLUME"]=scratch_total_size * 1024
            j.Variable_List["TORQUE_RESC_TOTAL_SCRATCH_VOLUME"]=scratch_total_size * 1024

    if e.type == pbs.EXECJOB_END:
        j = e.job
        conn = sqlite3.connect(sqlite_db)
        c = conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='resources'");
        if c.fetchone() != None:
            c.execute("DELETE FROM resources WHERE job_id='%s'" % j.id)
        conn.commit()
        conn.close()

    if e.type == pbs.EXECHOST_PERIODIC:
        dyn_res = {}

        def get_avail(scratch_type):
            global scratch_paths

            try:
                s = os.statvfs("/%s/" % scratch_paths[scratch_type])
                return (s.f_bsize * s.f_bavail) /1024
            except:
                return 0


        vnl = pbs.event().vnode_list
        local_node = pbs.get_local_nodename()

        for scratch_i in scratch_types.keys():
            dyn_res[scratch_i] = get_avail(scratch_i)
            vnl[local_node].resources_available[scratch_i] = pbs.size( "%skb" % dyn_res[scratch_i])
            pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, reporting %s %s %skb" % (local_node, scratch_i, dyn_res[scratch_i]))

        pbs.logmsg(pbs.EVENT_DEBUG, "scratch hook, reported %s" % str(dyn_res))

except SystemExit:
    pass
except:
    e.reject("scratch hook failed")
