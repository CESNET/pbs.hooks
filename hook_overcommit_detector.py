import pbs
import re
import smtplib
import socket

def sendmail(subject, body):
    sender = 'overcommit_detector@' + socket.getfqdn()
    receivers = ['vchlumsky@cesnet.cz']

    message = """From: """ + sender + """
To: """ + ",".join(receivers)+ """
Subject: """ + subject+ """

""" + body+ """
"""
    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(sender, receivers, message)         
    except SMTPException:
        pass

def parse_exec_vnode(exec_vnode):
    resources = {}
    for i in str(exec_vnode).split("+"):
        i = i.replace("(","")
        i = i.replace(")","")

        node_i = i.split(":")[0].split(".")[0]

        if not node_i in resources.keys():
            resources[node_i] = {}

        m = re.search('ncpus=([0-9]+?)', i)
        if m:
            if not 'ncpus' in resources[node_i].keys():
                resources[node_i]['ncpus'] = 0
            resources[node_i]['ncpus'] += int(m.group(1))

    return resources

try:
    e = pbs.event()
    if e.type == pbs.RUNJOB:
        j = e.job
        resources = parse_exec_vnode(j.exec_vnode)
        for nodename in resources.keys():
            node = pbs.server().vnode(nodename)

            available_ncpus = node.resources_available['ncpus']
            assigned_ncpus = node.resources_assigned['ncpus']

            try:
                 requested_ncpus = resources[nodename]['ncpus']
            except:
                 requested_ncpus = 1

            if assigned_ncpus + requested_ncpus > available_ncpus:
                msg = "overcommit_detector detected attempt to overcommit node %s by job %s" % (nodename, j.id)
                sendmail("pbs overcommit detector", msg);
                e.reject(msg)

except SystemExit:
    pass
except Exception as err:
    e.reject("overcommit_detector failed: %s" % str(err))
