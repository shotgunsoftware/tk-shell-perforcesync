
###!/usr/bin/env python
# Copyright (c) 2013 Shotgun Software Inc.
# 
# CONFIDENTIAL AND PROPRIETARY
# 
# This work is provided "AS IS" and subject to the Shotgun Pipeline Toolkit 
# Source Code License included in this distribution package. See LICENSE.
# By accessing, using, copying or modifying this work you indicate your 
# agreement to the Shotgun Pipeline Toolkit Source Code License. All rights 
# not expressly granted therein are reserved by Shotgun Software Inc.

import os
import sys
import time
import optparse
import traceback

from P4 import P4, P4Exception

class ShotgunSync(object):
    """
    """

    P4_COUNTER_NAME = "tk_shotgun_sync"    

    def __init__(self):
        """
        """
        self._debug_logging = True
        
        self.port = ""
        self.user = ""
        self.password = ""
        self._p4 = None
        
        # some useful cache info:        
        self._project_roots = set()
        self._project_pc_roots = {}
        self._pc_tk_instances = {}
        self._sg_user_lookup = {}
        
    def log_info(self, msg):
        print msg
    
    def log_debug(self, msg):
        if self._debug_logging:
            print msg
            
    def log_error(self, msg):
        print "Error: %s" % msg
        
        
    def sync_changes(self, start_change, end_change):
        """
        Sync range of changes:
        """
        self.log_info("Syncing changes %d - %d..." % (start, end))
        
        # connect to Perforce:
        if not self._connect():
            return
            
        try:
            # sync changes:
            for change_id in range(start_change, end_change+1):
                try:
                    self._sync_change(change_id)
                except Exception, e:
                    self.log_error("Failed to sync change %d: %s" % (change_id, e))   
        finally:
            # always disconnect:
            self._disconnect()
        
    def run_daemon(self, interval=30):
        """
        Run continuous daemon!
        """
        while True:
            
            self.log_info("Checking for new changes to sync with Shotgun...")
    
            try:
                # connect to Perforce:   
                if self._connect():     

                    start_change = None
                    end_change = None
                    
                    try:
                        # query start change from perforce counter:
                        p4_res = self._p4.run_counter(ShotgunSync.P4_COUNTER_NAME)
                        # [{'counter': 'tk_shotgun_sync', 'value': '0'}]
                        start_change = int(p4_res[0]["value"])+1 if p4_res else 0
                        
                        # get highest submitted change from perforce:
                        p4_res = self._p4.run_changes("-m", "1", "-s", "submitted")
                        # [{'status': 'submitted', 'changeType': 'public', 'change': '36', ...}]
                        end_change =  int(p4_res[0]["change"])
                    except P4Exception, e:
                        self.log_error("Failed to determine changelist range: %s" % p4.errors[0] if p4.errors else e)
                    except Exception, e:
                        self.log_error("Failed to determine changelist range: %s" % e)
                    
                    if (start_change != None and end_change != None and end_change >= start_change):
                        
                        # sync changes:
                        for change_id in range(start_change, end_change+1   ):
                            try:
                                self._sync_change(change_id)
                            except Exception, e:
                                self.log_error("Failed to sync change %d: %s" % (change_id, e))    
        
                        # update counter:                               
                        try:           
                            self._p4.run_counter(ShotgunSync.P4_COUNTER_NAME, str(end_change))
                        except Exception, e:
                            self.log_error("Failed to update counter '%s' - %s" % (ShotgunSync.P4_COUNTER_NAME, e))
                    else:
                        self.log_debug("No new changes found to sync!")
            finally:
                self._disconnect()
                        
            # and sleep for a bit:
            self.log_debug("Sleeping for %d seconds" % interval)
            time.sleep(interval)
        
        
    def _connect(self):
        """
        Open Perforce connection and log in
        """
        p4 = self._p4
        try:
            if not self._p4:
                p4 = P4()
                p4.exception_level = 1
                p4.port = self.port
                p4.user = self.user
            
            # if need to, connect:
            if not p4.connected():
                self.log_info("Connecting to Perforce...")
                p4.connect()
            
            # and finally, log in:
            p4.password = self.password
            p4.run_login()
        except P4Exception, e:
            self.log_error("Failed to connect to Perforce: %s" % (p4.errors[0] if p4.errors else e))
            return False
            
        self._p4 = p4
        return True    

    def _disconnect(self):
        """
        """
        if self._p4 and self._p4.connected():
            self._p4.disconnect()
        
    def _get_sg_user(self, tk, perforce_user):
        """
        Use hook to get user
        """
        
        if perforce_user not in self._sg_user_lookup:
            # (TODO) - move to hook/use the perforce framework..
            sg_res = tk.shotgun.find_one("HumanUser", [["sg_perforce_user", "is", perforce_user]], ["login"])
            if not sg_res:
                # try the login field instead:
                sg_res = tk.shotgun.find_one('HumanUser', [['login', 'is', perforce_user]])
            
            self._sg_user_lookup[perforce_user] = sg_res

        return self._sg_user_lookup[perforce_user] 
        
    def _get_context_for_path(self, tk, local_path):
        """
        Use hook to construct context for the path
        """
        # (TODO) - move to hook
        context = tk.context_from_path(local_path)
                
        # if we don't have a task but do have a step then try to determine the task from the step:
        if not context.task:
            if context.entity and context.step:
                sg_res = tk.shotgun.find("Task", [["step", "is", context.step], ["entity", "is", context.entity]])
                if sg_res and len(sg_res) == 1:
                    context = tk.context_from_entity(sg_res[0]["type"], sg_res[0]["id"])
        
        return context
        
    def _get_depot_project_root(self, depot_file):
        """
        Find the depot-relative project root for the specified depot file
        """
        # first, check to see if depot_file is under a known project root:
        for pr in self._project_roots:
            if depot_file.startswith(pr):
                return pr
        
        # start search from project root:
        project_root = depot_file
        
        while len(project_root):
            project_root = project_root[:project_root.rfind("/") or 0].rstrip("/")
            tank_configs_path = "%s/tank/config/tank_configs.yml" % project_root
            try:
                # see if this file exists in the depot:
                p4_res = self._p4.run_files(tank_configs_path)
                if p4_res:
                    # it does - win!
                    break
            except P4Exception:
                # ignore perforce exceptions!
                pass
        
        # cache project root for next time:
        if project_root:
            self._project_roots.add(project_root)
        
        return project_root
    
    
    def _get_local_pc_root(self, project_root):
        """
        Determine the local pipeline configuration directory
        for the given depot project_root... 
        """

        # first, check to see if info is in cache:
        pc_root = self._project_pc_roots.get(project_root)
        if pc_root != None:
            return pc_root
        self._project_pc_roots[project_root] = ""
        
        # check that the tank_configs.yml file is in the correct place:
        tank_configs_path = "%s/tank/config/tank_configs.yml" % project_root
        try:
            self._p4.run_files(tank_configs_path)
        except P4Exception:
            # bad - file not found!
            return None

        # read the pc root path from the config file:
        try:
            p4_res = self._p4.run_print(tank_configs_path)
            if not p4_res:
                return None
            
            # [{'rev': '1', ...}, "- {darwin: /toolkit_perforce/shotgun/zombie_racer_5, ..."]
            contents = p4_res[1]

            from tank_vendor import yaml
            config = yaml.load(contents)
            
            local_pc_root = config[0][sys.platform]
            
        except Exception, e:
            self.log_error("Failed to determine project root: %s" % e)
            # any exception is bad!
            return None
        
        # cache in case we need it again:
        self._project_pc_roots[project_root] = local_pc_root
        
        return local_pc_root 
        
        
    def _sync_change(self, change_id):
        """
        """
        self.log_info("Syncing change %d" % change_id)
    
        try:    
            # get the change details:
            p4_res = self._p4.run_describe(change_id)
    
            if not p4_res:
                return
            
            # p4_res = [
            #  {'status': 'submitted', 
            #   'fileSize': ['368095', '368097'], 
            #   'changeType': 'public', 
            #   'rev': ['3', '2'], 
            #   'client': 'ad_zombie_racer', 
            #   'user': 'Alan', 
            #   'time': '1382901628', 
            #   'action': ['edit', 'edit'], 
            #   'path': '//depot/projects/zombie_racer_5/master/assets/Environment/Track_Straight/Model/work/maya/*', 
            #   'digest': ['5BA1FC1E208D11BFE5C9CF61449026FF', 'DB747894FC9FE007A04863FEEA71CB17'], 
            #   'type': ['xtext', 'text'], 
            #   'depotFile': ['//depot/projects/zombie_racer_5/master/assets/Environment/Track_Straight/Model/work/maya/TrackStraight.ma', 
            #                 '//depot/projects/zombie_racer_5/master/assets/Environment/Track_Straight/Model/work/maya/TrackStraightB.ma'], 
            #   'change': '37', 
            #   'desc': 'lets submit a couple of files!\n'}
            # ]
            change_desc = p4_res[0]
            
            # construct new revision data:
            revision = {}
            revision["code"] = change_desc["change"]
            revision["description"] = change_desc["desc"]
            
            #revision["sg_client"]
            #revision["sg_files"]
            #revision["revision_links"]

            per_project_details = {}

            file_revs = zip(change_desc.get("depotFile", []), change_desc.get("rev", []))
            for depot_file, rev in file_revs:
                
                # Determine the depot project root for this depot file:
                depot_project_root = self._get_depot_project_root(depot_file)
                
                # then find the pipeline config root for the depot root:
                local_pc_root = self._get_local_pc_root(depot_project_root)
                
                # get a tk instance for this pc root:
                tk = self._pc_tk_instances.get(local_pc_root)
                if not tk:
                    tk = sgtk.sgtk_from_path(local_pc_root)
                    self._pc_tk_instances[local_pc_root] = tk
                
                # from this, get the shotgun project id:
                project_id = tk.pipeline_configuration.get_project_id()
                
                details = per_project_details.setdefault(project_id, {})
                details["tk"] = tk
                
                # get the shotgun user - concievably this could be different
                # across projects!:
                sg_user = self._get_sg_user(tk, change_desc["user"])
                details["user"] = sg_user
                                
                # build a context for the depot path:
                # (AD) - this is obviously very fragile so need a way to do this using depot paths
                # - maybe be able to set the project root and then set it to depot_project_root?
                local_path = tk.pipeline_configuration.get_primary_data_root() + depot_file[len(depot_project_root):]
                context = self._get_context_for_path(tk, local_path)    
                
                # And register publish for the file:
                # (AD) - this doesn't quite work as it tries to substitute the primary storage
                # into the depot path which fails!
                self.log_debug("Registering new published file: %s:%s" % (depot_file, rev))
                
                sg_res = sgtk.util.register_publish(tk, context, depot_file, os.path.basename(local_path), int(rev), comment = revision["description"], created_by = sg_user)
                
                file_data = {"depot":depot_file, "local":local_path, "context":context, "sg_file":sg_res}
                
                # and add to details
                details.setdefault("files", []).append(file_data)
            
            # create revision per project:
            for details in per_project_details.values():
                tk = details["tk"]
                files = details["files"]
                sg_user = details["user"]

                # build the revision data:               
                data = revision.copy()
                data["project"] = {"type":"Project", "id":project_id}
                data["created_by"] = sg_user
                
                # (AD) - TODO - this should handle legacy publoshed file types as well..
                data["sg_publishedfiles"] = [{"type":"PublishedFile", "id":file["sg_file"]["id"]} for file in files]
                
                self.log_debug("Creating new revision: %s" % data["code"])
                tk.shotgun.create('Revision', data)
            
        except Exception, e:
            self.log_error(e)



if __name__ == "__main__":
    """
    Main entry point for the script
    """
    try:
        
        # parse the command line:
        usage = "usage: %prog [options]"
        
        parser = optparse.OptionParser(usage=usage)
        parser.add_option("-d", "--daemon", help="Run as continuous daemon", action="store_true")
        parser.add_option("-i", "--interval", help="Interval between daemon cycles")
        parser.add_option("-c", "--changelist", help="Changelist id or range")
        parser.add_option("-p", "--port", help="Perforce server port")
        parser.add_option("-u", "--user", help="Perforce user")
        parser.add_option("-P", "--password", help="Perforce password")
        parser.add_option("-t", "--tkroot", help="Toolkit studio location")
        
        (options, args) = parser.parse_args()
        if args:
            parser.error("unkown args: %s" % args)

        # Construct sync object:
        sync = ShotgunSync()
        sync.port = options.port or ""
        sync.user = options.user or ""
        sync.password = options.password or ""
    
        # (AD) - temp!
        toolkit_python_path = os.path.join(options.tkroot, "install", "core", "python")
        #toolkit_python_path = "/toolkit_perforce/shotgun/studio/install/core/python"
        sys.path.append(toolkit_python_path)
        import sgtk
        from tank_vendor import yaml
    
        if options.daemon:
            # run as a daemon:
            if options.interval:
                sync.run_daemon(int(options.interval))
            else:
                sync.run_daemon()
        elif options.changelist:
            # run once for the specific changelist/range
            change = options.changelist
            change_range = change.split(":")
            start = int(change_range[0])
            end = max(start, int(change_range[-1]))

            # sync changes:
            sync.sync_changes(start, end)
        else:
            pass
                
    except Exception, e:
        print "Unhandled Exception: %s" % e
    
    
    
    
    
    
    
    
    