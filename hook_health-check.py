import pbs
import json
import os
import stat
import subprocess
import re


class HealthCheck(object):

    script = "/var/spool/pbs/mom_scripts/health-check"

    allowed_permission = "0700"
    allowed_uid = 0
    allowed_gid = 0

    comment_prefix = "HEALTH-CHECK: "
    comment = None

    rc_to_comment = {
        0: None, 
    }

    def __init__(self, e):
        self.rc = -1
        self.e = e

        self.parse_cfg()

        self.nodename = pbs.get_local_nodename()

        try:
            self.node = pbs.server().vnode(self.nodename)
        except:
            pbs.logmsg(pbs.EVENT_DEBUG, "Health-check hook; failed to get node info from server")
            self.e.reject()

        self.vnl = self.e.vnode_list

    def parse_cfg(self):
        config = {}
        if 'PBS_HOOK_CONFIG_FILE' in os.environ:
            config_file = os.environ["PBS_HOOK_CONFIG_FILE"]
            try:
                config = json.loads(open(config_file, 'r').read())
            except:
                pbs.logmsg(pbs.EVENT_DEBUG, "Health-check hook; failed to open config file, using defaults")
                config = {}

        try:
            if "health_check_file" in config.keys():
                self.script = str(config["health_check_file"])

            if "allowed_permission" in config.keys():
                self.allowed_permission = str(config["allowed_permission"])

            if "allowed_uid" in config.keys():
                self.allowed_uid = int(config["allowed_uid"])

            if "allowed_gid" in config.keys():
                self.allowed_gid = int(config["allowed_gid"])

            if "comment_prefix" in config.keys():
                self.comment_prefix = str(config["comment_prefix"])

            if "comments" in config.keys():
                self.rc_to_comment = {}
                for i in config["comments"]:
                    self.rc_to_comment[int(i)] = str(config["comments"][i])

        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "Health-check hook; failed to parse config '%s'" % err)
            self.e.reject()

    def file_check(self):

        if not os.path.isfile(self.script): 
            pbs.logmsg(pbs.EVENT_DEBUG, "Health-check hook; %s is not a file or not found" % self.script)
            return False
    
        file_permission = oct(stat.S_IMODE(os.lstat(self.script).st_mode))

        if file_permission != self.allowed_permission:
            pbs.logmsg(pbs.EVENT_DEBUG, "Health-check hook; incorrect file permission: %s" % file_permission)
            return False

        s = os.stat(self.script)

        if s.st_uid != self.allowed_uid:
            pbs.logmsg(pbs.EVENT_DEBUG, "Health-check hook; incorrect file owner: %d:%d" % (s.st_uid, s.st_gid))
            return False

        if s.st_gid != self.allowed_gid:
            pbs.logmsg(pbs.EVENT_DEBUG, "Health-check hook; incorrect file owner: %d:%d" % (s.st_uid, s.st_gid))
            return False

        return True

    def call_hc(self, script):
        stdout = None
        stderr = None

        my_env = os.environ.copy()
        my_env["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/bin:/usr/sbin"

        try:
            proc = subprocess.Popen(script,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    env=my_env,
                                    shell=True)
    
            stdout, stderr = proc.communicate()
            self.rc = proc.returncode
        except Exception as err:
            pbs.logmsg(pbs.EVENT_DEBUG, "Health-check hook; run script %s error: '%s'" % (script, err))
            self.e.reject()

        pbs.logmsg(pbs.EVENT_DEBUG, "Health-check hook; finished with exit code: %d" % self.rc)

        if stdout or stderr:
            pbs.logmsg(pbs.EVENT_DEBUG,
               "Health-check hook; stdout: '%s' stderr: '%s'" % (str(stdout).replace("\n", " "), str(stderr).replace("\n", " ")))

        if stdout:
            lines = stdout.strip().split("\n")
            self.comment = lines[len(lines)-1]

        if stderr:
            pbs.logmsg(pbs.EVENT_DEBUG, "Health-check hook; stderr not empty, skipping")
            self.e.reject()

    def get_prev_comment(self):
        if self.node.comment:
            return re.sub(self.comment_prefix + '.*', '', self.node.comment).strip()

        return ""

    def set_comment(self):
        if (self.rc <= 0):
            return

        comment = None

        if (self.rc in self.rc_to_comment.keys()):
            comment = self.rc_to_comment[self.rc]

        if (self.comment):
            comment = self.comment

        if comment:
            comment = self.get_prev_comment() + " " + self.comment_prefix + comment
            self.vnl[self.nodename].comment = comment.strip()

    def set_online(self):
        pbs.logmsg(pbs.EVENT_DEBUG,"Health-check hook; node is OK")

        self.vnl[self.nodename].state = pbs.ND_FREE

        if self.comment_prefix in str(self.node.comment):
            comment = self.get_prev_comment()
            if comment:
                self.vnl[self.nodename].comment = comment
            else:
                self.vnl[self.nodename].comment = None

    def set_offline(self):
        pbs.logmsg(pbs.EVENT_DEBUG,"Health-check hook; node is OFFLINE")

        self.vnl[self.nodename].state = pbs.ND_OFFLINE

        self.set_comment()

    def health_check(self):
        if self.e.type == pbs.EXECHOST_PERIODIC:

            self.call_hc(self.script)

            if self.rc == 0:
                self.set_online()

            if self.rc > 0:
                self.set_offline()

if __name__ == "__builtin__":
    pbs.logmsg(pbs.EVENT_DEBUG,"Health-check hook; starting the node health-check")

    hc = HealthCheck(pbs.event())
    if hc.file_check():
        hc.health_check()
    else:
        pbs.logmsg(pbs.EVENT_DEBUG,"Health-check hook; skipped")
