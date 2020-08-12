import pbs
import re
import os
import json

def parse_cfg():
    config = {}
    if 'PBS_HOOK_CONFIG_FILE' in os.environ:
        config_file = os.environ["PBS_HOOK_CONFIG_FILE"]
        try:
            config = json.loads(open(config_file, 'r').read())
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "node_owner; failed to open config file %s: %s" % (config_file, str(err)))
            config = {}
            return config

    return config

try:
    e = pbs.event()
    if e.type == pbs.QUEUEJOB:
        j = e.job
        node_owner = "everybody"

        config = parse_cfg()

        if "users" in config.keys():        
            if str(e.requestor) in config["users"]:
                node_owner = str(e.requestor)

        if "select" in j.Resource_List.keys():
            newselect = []
            for i in str(j.Resource_List["select"]).split("+"):
                if i:
                    i += ":node_owner=%s" % node_owner
                    newselect.append(i)

            if newselect:
                pbs.logmsg(pbs.LOG_DEBUG, "Old select: %s" % str(j.Resource_List))
                j.Resource_List["select"] = pbs.select("+".join(newselect))
                pbs.logmsg(pbs.LOG_DEBUG, "New select: %s" % str(j.Resource_List))

        elif "place" not in j.Resource_List.keys():
            j.Resource_List["node_owner"] = node_owner

except SystemExit:
    pass
except Exception as err:
    e.reject("node_owner hook failed with error: %s" % str(err))
