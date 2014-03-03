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
"""

import os
import sys
import urllib
from datetime import datetime
from pprint import pprint

import sgtk
from sgtk import TankError

p4_fw = sgtk.platform.get_framework("tk-framework-perforce")
p4_util = p4_fw.import_module("util")
from P4 import P4Exception

class ShotgunSync(object):
    """
    Handle syncronisation of Perforce changes with Shotgun
    """
    
    def __init__(self, app, p4_user=None, p4_pass=None):
        """
        Construction
        """
        self._app = app
        self.__p4_user = p4_user
        self.__p4_pass = p4_pass
        
        # some useful cache info:        
        self.__project_roots = set()
        self.__project_pc_roots = {}
        self.__pc_tk_instances = {}
        self.__sg_user_lookup = {}
        self.__depot_path_details_cache = {}
        
    def sync_changes(self, start_change, end_change):
        """
        Sync a range of changes with Shotgun
        
        :param start_change:    The first change to sync
        :param end_change:      The last change to sync
        """
        self._app.log_info("Syncing changes %d - %d..." % (start_change, end_change))
        
        # connect to Perforce:
        p4 = self.__connect_to_perforce()
        if p4:
            try:
                # sync changes:
                for change_id in range(start_change, end_change+1):
                    try:
                        self.__sync_change(change_id, p4)
                    except TankError, e:
                        self._app.log_error("Failed to sync change %d: %s" % (change_id, e))
                    except Exception, e:
                        self._app.log_exception("Failed to sync change %d: %s" % (change_id, e))
            finally:
                # always disconnect:
                p4.disconnect()

    def __sync_change(self, change_id, p4):
        """
        Sync a single change with Shotgun.
        
        :param change_id:    The id of the Perforce change to sync
        :param p4:           The Perforce connection to use
        """
        self._app.log_info("Syncing change %d" % change_id)
        
        # get the Perforce change:
        p4_change = None
        try:
            p4_res = p4.run_describe(change_id)
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
            if not p4_res:
                return
            
            p4_change = p4_res[0]
            if p4_change.get("status") != "submitted":
                # only care about submitted changes
                return
            
            # if change isn't in this project then skip:
            if not self.is_change_in_context(p4, p4_change):
                return
            
        except P4Exception, e:
            self._app.log_error("Failed to query perforce change %d: %s" 
                                % (change_id, p4.errors[0] if p4.errors else e))
            return
        
        # next, create a Revision entity in Shotgun for this change:
        sg_change = self.create_sg_entity_for_change(p4_change)
        if not sg_change:
            return
        
        # finally, update the contents of the change in Shotgun:
        self.sync_change_contents(p4, p4_change, sg_change)

    def is_change_in_context(self, p4, p4_change):
        """
        Determine if the specified change is valid for the current context (project).
        This will check that at least one file in the change is within the project root
        
        :param p4_change:    The Perforce changelist to check
        """ 
        for depot_path in p4_change.get("depotFile", []):

            # find the depot root and tk instance for the depot path:
            details = self.__find_file_details(depot_path, p4)
            if not details:
                return
            _, tk = details
            
            if tk.pipeline_configuration.get_project_id() == self._app.context.project["id"]:
                # this file is in the project, therefore so is the change!
                return True
            
        # none of the files in the change are in this project!
        return False
        

    def create_sg_entity_for_change(self, p4_change):
        """
        Create a 'Revision' entity for a Perforce change in Shotgun.  A change
        is created and then the matching change with the lowest id is retrieved
        to ensure this process was the first to create the entity.
        
        If it wasn't then it deletes the entity imediately and returns nothing
        as it's assumed that another process created this change first. 
        
        :param p4_change: The Perforce change to be populated in Shotgun
        """
        if not p4_change:
            return

        change_id = str(p4_change["change"])
        created_at = datetime.fromtimestamp(int(p4_change["time"]))
        
        self._app.log_info("Creating Shotgun Revision entity for Perforce change %s" % change_id)        
        
        # check to see if this change exists in Shotgun:
        try:
            sg_res = self._app.shotgun.find_one("Revision", [["project", "is", self._app.context.project], ["code", "is", change_id]])
            if sg_res:
                # change already exists for this project and we
                # don't want to create it twice!
                self._app.log_debug("Shotgun Revision entity for Perforce change %s already exists!" % change_id)        
                return
        except Exception, e:
            self._app.log_error("Failed to query change from Shotgun: %s" % e)
            return
        
        # it doesn't exist so lets create it:
        sg_change = None
        try:
            sg_user = self.__get_sg_user(self._app.sgtk, p4_change["user"])        
        
            change_data = {}
            change_data["code"] = change_id
            change_data["description"] = p4_change.get("desc", "")
            change_data["project"] = self._app.context.project
            change_data["created_by"] = sg_user
            change_data["created_at"] = created_at 
            change_data["sg_workspace"] = p4_change.get("client")
            
            sg_change = self._app.shotgun.create("Revision", change_data)
        except Exception, e:
            self._app.log_error("Failed to create change (Revision) entity in Shotgun: %s" % e)
            return
        
        # next check that the entity created is the most recent one - this is so that
        # we can ensure nothing else created it at the same time!
        try:
            # find the revision entity for our change with the lowest id:
            sg_first_change = self._app.shotgun.find_one("Revision", 
                                            [["project", "is", self._app.context.project], ["code", "is", change_id]],
                                            order = [{"field_name":"id", "direction":"asc"}])
            if not sg_first_change:
                self._app.log_error("Failed to find newly created change (Revision) entity %s in Shotgun!" % change_id)
                return
            elif sg_first_change["id"] != sg_change["id"]:
                # someone else got there first so lets delete the change we just created!
                if not self._app.shotgun.delete("Revision", sg_change["id"]):
                    self._app.log_error("Failed to delete change (Revision) entity %s in Shotgun - please fix manually!" % change_id)
                return
        except Exception, e:
            self._app.log_error("Failed to determine if the Revision entity (id: %d) is the only "
                                "entity registered for Perforce change %s - please check manually!" 
                                % (sg_change["id"], change_id))
            
        # ok, the change we created is the only valid entity so return it:
        self._app.log_debug("Successfully created Shotgun Revision entity %d for Perforce change %s" 
                            % (sg_change["id"], change_id))
        return sg_change


    def sync_change_contents(self, p4, p4_change, sg_change_entity):
        """
        Sync the files modified in the specified Perforce change to the specified
        Shotgun Revision entity.
        
        :param p4:                  The Perforce connection to use
        :param p4_change:           The Perforce change description to sync
        :param sg_change_entity:    The Shotgun Revision entity
        """
        
        sg_user = self.__get_sg_user(self._app.sgtk, p4_change["user"])
        change_id = str(p4_change["change"])
        
        # get details for all files in change excluding any deletes, move/deletes, etc.:
        p4_res = []
        try:
            p4_res = p4.run_fstat("-T", "depotFile, headRev", 
                                  "-F", "^headAction=delete ^headAction=move/delete ^headAction=purge ^headAction=archive",                                  
                                  "-e", change_id, 
                                  "//...")
        except P4Exception, e:
            self._app.log_error("Failed to query file revisions for change %s: %s" 
                                % (change_id, p4.errors[0] if p4.errors else e))
            return

        file_revs = set()
        for item in p4_res:
            depot_file = item.get("depotFile")
            head_rev = item.get("headRev")
            if not depot_file or not head_rev:
                continue
            
            file_revs.add((depot_file, head_rev))
        
        # process all remaining file revisions for the change:
        published_files = []
        for depot_path, rev in file_revs:
            
            # process file revision
            # (AD) - this probably wants to be split into two passes so that we can
            # handle dependencies where the dependent file is part of the same change.
            # ...            
            published_file = self.__process_file_revision(depot_path, int(rev), sg_user, p4_change, p4)

            if published_file:
                published_files.append(published_file)          
        
        if not published_files:
            # nothing else to do
            return
            
        published_file_entity_type = sgtk.util.get_published_file_entity_type(self._app.sgtk)
        
        # (TEMP) - whilst installing for testing, I messed up when creating the sg_published_files field on the Revision
        # entity, creating it with the wrong type!
        # Until this is fixed, we need to check here to see if sg_publishedfiles should be used instead!
        if not hasattr(self, "__published_file_field"):
            pf_field = None
            for field in ["sg_published_files", "sg_publishedfiles"]:
                try:
                    schema = tk.shotgun.schema_field_read("Revision")[field]
                    if (schema.get("data_type", {}).get("value") == "multi_entity"
                        and published_file_entity_type in schema.get("properties", {}).get("valid_types", {}).get("value", [])):
                        # ok to use this field!
                        pf_field = field
                        break
                except:
                    pass
            # default to the 'correct' field anyway!
            self.__published_file_field = pf_field or "sg_published_files"                
        
        # build the update data for the change:
        change_data = {self.__published_file_field:[]}
        for pf in published_files:
            change_data[self.__published_file_field].append({"type":published_file_entity_type, "id":pf["id"]})
            
        # update the change:
        self._app.log_debug("Updating Published files for change (Revision) entity %s..." % (sg_change_entity["code"]))
        try:
            self._app.shotgun.update("Revision", sg_change_entity["id"], change_data)
        except Exception, e:
            self._app.log_error("Failed to update revision entity %d - %s" % (sg_change_entity["id"], e))
        
    def __find_file_details(self, depot_path, p4):
        """
        Find the depot project root and tk instance for the specified depot path
        if possible.
        
        :param depot_path:    Depot path to check
        :param p4:            Perforce connection to use
        :returns:             Tuple containing (depot project root, sgtk instance)
        """
        # first, check the cache to see if we found this information previously:
        if depot_path in self.__depot_path_details_cache:
            return self.__depot_path_details_cache.get(depot_path)
        self.__depot_path_details_cache[depot_path] = None
        
        # Determine the depot project root for this depot file:
        depot_project_root = self.__get_depot_project_root(depot_path, p4)
        if not depot_project_root:
            # didn't find a project root so this file is probably not
            # within a Toolkit data directory!
            return

        # find the pipeline config root from the depot root:
        local_pc_root = self.__get_local_pc_root(depot_project_root, p4)
        if not local_pc_root:
            # didn't find a matching pipeline configuration location!
            self._app.log_error("Failed to locate pipeline configuration for depot project root '%s'" 
                                % depot_project_root)
            return

        # get a tk instance for this pipeline configuration:
        tk = self.__pc_tk_instances.get(local_pc_root)
        if not tk:
            # create a new api instance:
            tk = sgtk.sgtk_from_path(local_pc_root)
            self.__pc_tk_instances[local_pc_root] = tk

        res = (depot_project_root, tk)
        self.__depot_path_details_cache[depot_path] = res
        
        return res

    def __process_file_revision(self, depot_path, file_revision, sg_user, p4_change, p4):
        """
        Process a single revision of a perforce file.  If the file can be mapped to the
        current context's project then create a published file for it and return the
        details. 
        """
        
        # find the depot root and tk instance for the depot path:
        details = self.__find_file_details(depot_path, p4)
        if not details:
            return
        depot_project_root, tk = details
        
        # check that this tk instance is for the same project we're running in:
        if tk.pipeline_configuration.get_project_id() != self._app.context.project["id"]:
            # it isn't so we can skip this file:
            return None
        
        # it is so lets Register a new Published File for it if there
        # isn't one already
        #
        
        # check all data roots to see if this is a recognized toolkit file.  If no valid template
        # can be found then assume that the file is outside all data roots.
        template = None
        proxy_local_path = None
        depot_root_relative_path = depot_path[len(depot_project_root):].lstrip("\\/")
        # make sure we use the unquoted version of the depot path:
        depot_root_relative_path = urllib.unquote(depot_root_relative_path)
        for data_root in tk.roots.values():
            proxy_local_path = os.path.join(data_root, depot_root_relative_path)
            try:
                template = tk.template_from_path(proxy_local_path)
                if template:
                    break
            except:
                # (TODO) might want to handle certain errors here, e.g. matching multiple templates?
                pass
        if not template:
            self._app.log_debug("File '%s' is not recognized by toolkit, skipping" % depot_path)
            return None        
        
        # we'll use the file name for the publish name:
        publish_name = os.path.basename(proxy_local_path)
        
        # construct full url for the published file:
        file_url = p4_util.url_from_depot_path(p4, depot_path, file_revision)        
        
        # find existing publish entity if there is one:
        pf_entity_type = sgtk.util.get_published_file_entity_type(self._app.sgtk)
        filters = [["project", "is", self._app.context.project],
                   ["code", "is", publish_name],
                   ["version_number", "is", file_revision]]
        sg_res = self._app.shotgun.find(pf_entity_type, filters, ["code", "id", "path", "version"])
        sg_published_file = None
        for item in sg_res:
            if item.get("path", {}).get("url") == file_url:
                # publish has already been registered for this file
                sg_published_file = item
                break
        if sg_published_file:
            self._app.log_debug("Published file already exists for %s#%d" % (depot_path, file_revision))
            
        else:
            # need to register a new published file:
            
            # load any publish data we have stored for this file:
            publish_data = {}
            try:
                publish_data = p4_fw.load_publish_data(depot_path, sg_user, p4_change.get("client", ""), file_revision)
            except TankError, e:
                self._app.log_error("Failed to load publish data for %s#%d: %s" % (depot_path, file_revision, e))
            except Exception, e:
                self._app.log_exception("Failed to load publish data for %s#%d" % (depot_path, file_revision))
            
            context = publish_data.get("context")
            if not context:
                # try to build a context from the depot path:
                # (AD) - this is obviously very fragile so need a way to do this using depot paths
                # - maybe be able to set the project root and then set it to depot_project_root?
                # - this would also allow template_from_path to work on depot paths...
                context = self.__get_context_for_path(tk, proxy_local_path)
                
            if not context:
                self._app.log_error("Failed to determine context to use for %s - unable to register publish!" % depot_path)
                return None
            
            if not context.project:
                self._app.log_error("Failed to determine project to use for %s - unable to register publish!" % depot_path)
                return None
                
            # update optional args and register publish:
            publish_data["comment"] = p4_change.get("desc", "") # Always use change list description for the comment!
            publish_data["created_by"] = sg_user
            # (TODO) - should this be the file revisions headModTime?
            publish_data["created_at"] = datetime.fromtimestamp(int(p4_change["time"]))            
            publish_data["tk"] = self._app.sgtk
            publish_data["context"] = context
            publish_data["path"] = file_url
            publish_data["name"] = publish_name
            publish_data["version_number"] = file_revision
            
            self._app.log_info("Registering new published file: %s#%d" % (depot_path, file_revision))
            file_data = {}
            try:
                # (AD) Some notes about using register_publish with this data:
                #
                # - abstract fields won't get translated - if we do need this then we'll have to figure
                #   out how to handle it for this use case - non-trivial!
                # - dependencies_paths won't get resolved correctly so dependency_ids should be used 
                #   instead.  This also ensure the correct version is linked
                sg_published_file = sgtk.util.register_publish(**publish_data)
            except Exception, e:
                self._app.log_error("Failed to register publish for '%s': %s" % (depot_path, e))
                return None

        # now, check to see if we have version data:
        # (TODO) - not finished yet!
        version_entity = sg_published_file.get("version")
        if version_entity:
            # ?
            pass
        else:
            # may need to create version:
            pass

        # return the published file entity:
        return {"type":sg_published_file["type"], "id":sg_published_file["id"]}

    def __connect_to_perforce(self):
        """
        Connect to Perforce
        """
        try:
            p4 = p4_fw.connect(False, self.__p4_user, self.__p4_pass)
            return p4
        except:
            self._app.log_exception("Failed to connect!")
            return None
        
    def __get_sg_user(self, tk, perforce_user):
        """
        Get the Shotgun user for the specified Perforce user
        """
        if perforce_user not in self.__sg_user_lookup:
            # user not in lookup so ask framework:
            sg_user = p4_fw.get_shotgun_user(perforce_user)
            self.__sg_user_lookup[perforce_user] = sg_user
            return sg_user
        else:
            return self.__sg_user_lookup[perforce_user]
        
    def __get_context_for_path(self, tk, local_path):
        """
        Use hook to construct context for the path
        """
        # Although the context should ideally be preserved through the publish data when published, we
        # still need to handle the case where the file may have been submitted directly through Perforce 
        # so will probably still want to have hook for this...
        
        context = self._app.sgtk.context_from_path(local_path)
                
        # if we don't have a task but do have a step then try to determine the task from the step:
        # (TODO) - this logic should be moved to a hook as it won't work if there are Multiple tasks on 
        # the same entity that use the same Step!
        if context and not context.task:
            if context.entity and context.step:
                sg_res = self._app.shotgun.find("Task", [["step", "is", context.step], ["entity", "is", context.entity]])
                if sg_res and len(sg_res) == 1:
                    context = self._app.sgtk.context_from_entity(sg_res[0]["type"], sg_res[0]["id"])
        
        return context
        
    def __get_depot_project_root(self, depot_path, p4):
        """
        Find the depot-relative project root for the specified depot file
        """
        # first, check to see if depot_path is under a known project root:
        for pr in self.__project_roots:
            if depot_path.startswith(pr):
                return pr
        
        # start search from project root:
        project_root = depot_path
        
        while len(project_root):
            project_root = project_root[:project_root.rfind("/") or 0].rstrip("/")
            if not project_root:
                break
            
            # (AD) - this is going to change with the new path cache implementation
            tank_configs_path = "%s/tank/config/tank_configs.yml" % project_root
            try:
                # see if this file exists in the depot:
                p4_res = p4.run_files(tank_configs_path)
                if p4_res:
                    # it does - win!
                    break
            except P4Exception:
                # ignore perforce exceptions!
                pass
        
        # cache project root for next time:
        if project_root:
            self.__project_roots.add(project_root)
        
        return project_root
    
    
    def __get_local_pc_root(self, project_root, p4):
        """
        Determine the local pipeline configuration directory
        for the given depot project_root... 
        """

        # first, check to see if info is in cache:
        pc_root = self.__project_pc_roots.get(project_root)
        if pc_root != None:
            return pc_root
        self.__project_pc_roots[project_root] = ""
        
        # check that the tank_configs.yml file is in the correct place:
        tank_configs_path = "%s/tank/config/tank_configs.yml" % project_root
        try:
            p4.run_files(tank_configs_path)
        except P4Exception, e:
            # bad - file not found!
            self._app.log_error("Configuration file '%s' does not exist in the Perforce depot: %s" 
                           % (tank_configs_path, p4.errors[0] if p4.errors else e))
            return None

        # read the pc root path from the config file:
        try:
            p4_res = p4.run_print(tank_configs_path)
            if not p4_res:
                return None
            
            # [{'rev': '1', ...}, "- {darwin: /toolkit_perforce/shotgun/zombie_racer_5, ..."]
            contents = p4_res[1]

            from tank_vendor import yaml
            config = yaml.load(contents)
            
            # (TODO) - this currently uses the first pc root it finds which
            # isn't good - instead it should be looking for the pc that corresponds
            # to the current context/config that this command is being run in
            local_pc_root = config[0][sys.platform]

        except P4Exception, e:
            self._app.log_error("Failed to determine project root: %s" % (p4.errors[0] if p4.errors else e))
            return None
        except Exception, e:
            self._app.log_error("Failed to determine project root: %s" % e)
            return None
        
        # cache in case we need it again:
        self.__project_pc_roots[project_root] = local_pc_root
        
        return local_pc_root 
            