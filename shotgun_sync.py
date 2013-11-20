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

                    

"""
p4 review:

# use this to query changes 
p4 review -t shotgun-sync -c 6
p4 review -t shotgun-sync

# and in python
from P4 import P4

p4 = P4()

p4.port = "Alans-Macbook-Pro.local:1668"
p4.user = "Alan"
p4.password = "asdasdasdas"

p4.connect()

p4.run("review", "-t", "shotgun-sync")

# to create and save a new change:
change = p4.fetch_change()
change["Description"] = "..."
...
change_res = p4.save_change(change)
# ['Change 14 created.']

# fetch existing change - status will be 'pending'
change = p4.fetch_change(14)

# is p4 connected:
p4.connected()



"""

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
        self.host = ""
        self.port = ""
        self.user = ""
        self.password = ""
        self._p4 = None
        
        # some useful cache info:        
        self._project_roots = set()
        self._project_pc_roots = {}
        self._pc_tk_instances = {}
        self._sg_user_lookup = {}
        
        
    def sync_changes(self, start_change, end_change):
        """
        Sync range of changes:
        """
        print "Syncing changes %d - %d..." % (start, end)
        
        # connect to Perforce:
        if not self._connect():
            return
            
        try:
            # sync changes:
            for change_id in range(start_change, end_change+1):
                try:
                    self._sync_change(change_id)
                except Exception, e:
                    print "Failed to sync change %d: %s" % (change_id, e)   
        finally:
            # always disconnect:
            self._disconnect()
        
    def run_daemon(self, interval=30):
        """
        Run continuous daemon!
        """
        while True:
            
            print "Checking for new changes to sync with Shotgun..."
    
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
                    except Exception, e:
                        print "Failed to determine changelist range: %s" % e
                    
                    if (start_change != None and end_change != None
                        and end_change >= start_change):
                        
                        print "Found new changes to sync: %d - %d" % (start_change, end_change)
                        
                        # sync changes:
                        for change_id in range(start_change, end_change+1   ):
                            try:
                                self._sync_change(change_id)
                            except Exception, e:
                                print "Failed to sync change %d: %s" % (change_id, e)    
        
                        # update counter:                               
                        try:           
                            self._p4.run_counter(ShotgunSync.P4_COUNTER_NAME, str(end_change))
                        except Exception, e:
                            print "Failed to update counter '%s' - %s" % (ShotgunSync.P4_COUNTER_NAME, e)
                    else:
                        print "No new changes found to sync!"
            finally:
                self._disconnect()
                        
            # and sleep for a bit:
            print "Sleeping for %d seconds" % interval
            time.sleep(interval)
        
        
    def _connect(self):
        """
        """
        p4 = self._p4
        try:
            if not self._p4:
                p4 = P4()
                p4.host = self.host
                p4.port = self.port
                p4.user = self.user
            
            # if need to, connect:
            if not p4.connected():
                print "Connecting to Perforce..."
                p4.connect()
            
            # and finally, log in:
            p4.password = self.password
            p4.run_login()
        except P4Exception, e:
            print "Failed to connect: %s" % e
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
        # call a hook!
        sg_login = None
        if perforce_user == "Alan":
            sg_login = "Alan_Dann"
        else:
            sg_login = perforce_user
        
        if sg_login not in self._sg_user_lookup:
            sg_res = tk.shotgun.find_one('HumanUser', [['login', 'is', sg_login]])
            self._sg_user_lookup[sg_login] = sg_res

        return self._sg_user_lookup[sg_login] 
        
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
                self._p4.run_files(tank_configs_path)
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
        """
        try:
            p4_res =     .run_print(tank_configs_path)
            if not p4_res:
                return None
            
            # [{'rev': '1', ...}, "- {darwin: /toolkit_perforce/shotgun/zombie_racer_5, ..."]
            contents = p4_res[1]
            
            import yaml
            config = yaml.load(contents)
            
            
            local_pc_root = config[0][sys.platform]
            
        except:
            # any exception is bad!
            return None
        """
        # (AD) - TEMP
        local_pc_root = "/toolkit_perforce/shotgun/zombie_racer_5" 
        
        # cache in case we need it again:
        self._project_pc_roots[project_root] = local_pc_root
        
        return local_pc_root 
        
        
    def _sync_change(self, change_id):
        """
        """
        print "Syncing change %d" % change_id
    
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
            #revision["created_by"] = self._get_sg_user(change_desc["user"])
            
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
                                
                # try constructing a context from the file path:
                # (AD) - this is obviously very fragile so need a way to do this using depot paths
                # - maybe be able to set the project root and then set it to depot_project_root?
                local_path = tk.pipeline_configuration.get_primary_data_root() + depot_file[len(depot_project_root):]
                context = tk.context_from_path(local_path)
                
                # And register publish for the file:
                # (AD) - this doesn't quite work as it tries to substitute the primary storage
                # into the depot path which fails!
                print "Registering new revision: %s:%s" % (depot_file, rev)
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
                data["sg_publishedfiles"] = [{"type":"PublishedFile", "id":file["sg_file"]["id"]} for file in files]
                
                print "Creating new revision: %s" % data["code"]
                tk.shotgun.create('Revision', data)
                
            
 
            """
            1. Find local pc_root for project:
                
                //depot/.../project/tank/config/tank_configs.yml
                    --> points to pipeline configuration local root, e.g. //toolkit_perforce/shotgun/zombie_racer_5

                (Note - there may be multiple pc's defined but have no mechanism to choose which to use so for now
                just use primary!)
                
            2. Instantiate tk instance for pc_root:

                tk = sgtk.sgtk_from_path(pc_root)
            
                Q. Will this give me a project entity & root?
            
            
            3. Determine local project root and construct local path from depot path
            
                For now, just do string substitution but will instead need to map local client for script
                and use that as the project root.
            
            4. Determine context for local path:
            
                ctx = tk.context_from_path(local_file_path)
                
            5. Determine entity, etc.
            
                May need to do this through a hook so that additional context info can be
                determined (e.g. task from step if not contained within the path)
                
            6. Create new PublishedFile entity
            """
                
    
            """
            project_name = tk.execute_hook('perforce_project_from_path', path=path)
            
            # fill out basic information
            data = projects.setdefault(project_name, {
                'code': desc['change'],
                'sg_client': desc['client'],
                'description': desc['desc'],
                'sg_files': [],
                'revision_links': [],
            })
            data['sg_files'].append('%s#%s' % (desc['depotFile'][i], desc['rev'][i]))
            
            # only do user lookup once
            if not 'created_by' in data:
                data['created_by'] = tk.shotgun.find_one('HumanUser', [['login', 'is', desc['user']]])
            
            # see if there is an entity to link to
            ctx = tk.execute_hook('perforce_context_from_path', path=path)
            if ctx is not None:
                if ctx.task is not None:
                    data['revision_links'].append(ctx.task)
                if ctx.entity is not None:
                    data['revision_links'].append(ctx.entity)
                data['revision_links'].extend(ctx.additional_entities)
                project_entities[project_name] = ctx.project
            """

            
        except Exception, e:
            print e



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
        parser.add_option("-H", "--host", help="Perforce server")
        parser.add_option("-p", "--port", help="Perforce server port")
        parser.add_option("-u", "--user", help="Perforce user")
        parser.add_option("-P", "--password", help="Perforce password")
        
        (options, args) = parser.parse_args()
        if args:
            parser.error("unkown args: %s" % args)

        # Construct sync object:
        sync = ShotgunSync()
        sync.host = options.host or os.environ.get("P4HOST", "")
        sync.port = options.port or os.environ.get("P4PORT", "")
        sync.user = options.user or os.environ.get("P4USER", "")
        sync.password = options.password or os.environ.get("P4PASSWD", "")
    
        # (AD) - temp!
        toolkit_python_path = "/toolkit_perforce/shotgun/studio/install/core/python"
        sys.path.append(toolkit_python_path)
        import sgtk
    
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
        print e
    
    
    
    
    
    
    
    
    