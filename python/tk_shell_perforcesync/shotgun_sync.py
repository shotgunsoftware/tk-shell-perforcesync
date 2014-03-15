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
Contains functionality to sync a change and all the revisions of files it contains
with Shotgun
"""

import os
import sys
import urllib
from datetime import datetime
from pprint import pprint

import sgtk
from sgtk import TankError

p4_fw = sgtk.platform.get_framework("tk-framework-perforce")
from P4 import P4Exception

class ShotgunSync(object):
    """
    Handle syncronisation of Perforce changes with Shotgun
    """
    
    # (TODO) - this is going to change with the new path cache implementation due in v0.15 of the core!    
    CONFIG_BACK_MAPPING_FILE_LOCATION = "tank/config/%s" % sgtk.platform.constants.CONFIG_BACK_MAPPING_FILE 
    
    def __init__(self, app, p4_user=None, p4_pass=None):
        """
        Construction
        
        :param app:            The app bundle that constucted this object
        :param p4_user:        The Perforce user that the command should be run under
        :param p4_pass:        The Perforce password that the command should be run under        
        """
        self._app = app
        self.__p4_user = p4_user
        self.__p4_pass = p4_pass
        
        # some useful cache info:        
        self.__project_roots = set()
        self.__project_pc_roots = {}
        self.__pc_tk_instances = {}
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
        
        :param p4:           The Perforce connection to use
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
            sg_user = self.__get_sg_user(p4_change["user"])        
        
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
        change_id = str(p4_change["change"])
        
        # get details for all files in change excluding any deletes, move/deletes, etc.
        p4_res = []
        try:
            p4_res = p4.run_fstat("-T", "depotFile, headRev, headModTime", 
                                  "-F", "^headAction=delete ^headAction=move/delete ^headAction=purge ^headAction=archive",                                  
                                  "-e", change_id, 
                                  "//...")
        except P4Exception, e:
            self._app.log_error("Failed to query file revisions for change %s: %s" 
                                % (change_id, p4.errors[0] if p4.errors else e))
            return

        p4_file_details = {}
        for p4_file in p4_res:
            depot_file = p4_file.get("depotFile")
            head_rev = p4_file.get("headRev")
            if not depot_file or not head_rev:
                continue
            
            p4_file_details[(depot_file, int(head_rev))] = p4_file
        
        # process all remaining file revisions for the change, return a list of
        # corresponding Shotgun entities:
        published_file_entities = self.__process_file_revisions(p4, p4_file_details, p4_change)
        if not published_file_entities:
            return
        
        # ----------------------------------------------------------------------------------------------
        # (TEMP) - whilst installing for testing, I messed up when creating the sg_published_files field 
        # on the Revision entity, creating it with the wrong type!
        # Until this is fixed, we need to check here to see if sg_publishedfiles should be used instead!
        # Note: this won't affect other installs so it's safe to leave in here
        published_file_entity_type = sgtk.util.get_published_file_entity_type(self._app.sgtk)
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
        # ----------------------------------------------------------------------------------------------                
        
        # build the update data for the change:
        change_data = {self.__published_file_field:[]}
        for pf in published_file_entities:
            change_data[self.__published_file_field].append({"type":pf["type"], "id":pf["id"]})
            
        # update the change:
        self._app.log_debug("Updating Published files for change (Revision) entity %s..." % (sg_change_entity["code"]))
        try:
            self._app.shotgun.update("Revision", sg_change_entity["id"], change_data)
        except Exception, e:
            self._app.log_error("Failed to update revision entity %d - %s" % (sg_change_entity["id"], e))

    def __process_file_revisions(self, p4, p4_file_details, p4_change):
        """
        Process all file revisions for a change.
        """

        # pull some useful info from the change:
        sg_user = self.__get_sg_user(p4_change["user"])
        change_id = int(p4_change["change"])        
        change_client = p4_change.get("client", "")
        change_desc = p4_change.get("desc", "")
        change_time = datetime.fromtimestamp(int(p4_change["time"]))

        temporary_files = set()
        try:
            
            # --------------------------------------------------------------------------------------------
            # --------------------------------------------------------------------------------------------
            # FIRST PASS:
            # - filter files to just those that Toolkit can process
            # - find any existing entities for the files from Shotgun.
            # - Register a new publish for any new files in Shotgun,  
            #   separating out any dependency information
            publish_entities = {} # list of published entities
            new_publish_dependencies = {} # dependency details for new publishes
            new_publish_review_data = {}
            
            for path_revision, p4_file in p4_file_details.iteritems():
    
                (depot_path, file_revision) = path_revision
                
                self._app.log_debug("Processing %s#%d" % path_revision)
                
                # first, check that the depot path is a Toolkit file:
                path_is_valid, path_context = self.__validate_depot_path(depot_path, p4) 
                if not path_is_valid:
                    self._app.log_info("File '%s#%d' is not recognized by toolkit, skipping" % path_revision)
                    continue
                
                # find existing publish entity if there is one:
                sg_published_file = self.__find_publish_entity(depot_path, file_revision)
                if not sg_published_file:
                    # Didn't find a published file so lets gather the data ready to be able to create one...
                    #
                    publish_data = {}
                    
                    # load any publish data we have stored for this file:
                    try:
                        load_res = p4_fw.load_publish_data(depot_path, sg_user, change_client, file_revision)
                        if load_res and isinstance(load_res, dict):
                            publish_data = load_res.get("data", {})
                            temporary_files.update(load_res.get("temp_files", []))
                    except TankError, e:
                        self._app.log_error("Failed to load publish data for %s#%d: %s" % (depot_path, file_revision, e))
                        continue
                    except Exception, e:
                        self._app.log_exception("Failed to load publish data for %s#%d" % path_revision)
                        continue
                    
                    # try to ensure we have a valid context for the published file:
                    context = publish_data.get("context")
                    if not context:
                        # Fall back to the context that was built from the path
                        context = path_context
                    if not context:
                        self._app.log_error("Failed to determine context to use for %s#%d - unable to register publish!" 
                                            % path_revision)
                        continue
                    if not context.project:
                        self._app.log_error("Failed to determine project to use for %s#%d - unable to register publish!" 
                                            % path_revision)
                        continue
                        
                    # update publish data with additional information:
                    publish_data["name"] = os.path.basename(depot_path)            
                    publish_data["path"] = p4_fw.util.url_from_depot_path(depot_path, file_revision)
                    publish_data["version_number"] = file_revision
                    publish_data["comment"] = change_desc # Always use change list description for the comment!
                    publish_data["created_by"] = sg_user
                    publish_data["tk"] = self._app.sgtk
                    publish_data["context"] = context
        
                    publish_time = change_time
                    head_mod_time = p4_file.get("headModTime")
                    if head_mod_time:
                        publish_time = datetime.fromtimestamp(int(head_mod_time))
                    publish_data["created_at"] = publish_time            
                    
                    # extract the dependency data from the publish data - we'll
                    # update this later once everything has been registered           
                    dependency_ids = dependency_paths = []
                    if "dependency_ids" in publish_data:
                        dependency_ids = publish_data["dependency_ids"]
                        del(publish_data["dependency_ids"])
                    if "dependency_paths" in publish_data:
                        dependency_paths = publish_data["dependency_paths"]
                        del(publish_data["dependency_paths"])
                    new_publish_dependencies[path_revision] = {"ids":dependency_ids, "paths":dependency_paths}
        
                    # register the new publish:
                    self._app.log_info("Registering new published file: %s#%d" % path_revision)
                    sg_published_file = None
                    try:
                        # Some notes about using register_publish with this data:
                        # Note: Abstract fields won't get translated - if we need this functionality then 
                        # we'll have to figure out how to handle it for this use case - non-trivial!
                        sg_published_file = sgtk.util.register_publish(**publish_data)
                    except Exception, e:
                        self._app.log_error("Failed to register publish for '%s': %s" % (depot_path, e))
                        continue
                    
                    publish_entities[path_revision] = {"type":sg_published_file["type"], "id":sg_published_file["id"]}
                    
                    # Finally, look for any review data to be registered for this published file:
                    review_data = {}
                    try:
                        load_res = p4_fw.load_publish_review_data(depot_path, sg_user, change_client, file_revision)
                        if load_res and isinstance(load_res, dict):
                            review_data = load_res.get("data")
                            temporary_files.update(load_res.get("temp_files", []))                        
                    except TankError, e:
                        self._app.log_error("Failed to load review data for %s#%d: %s" % (depot_path, file_revision, e))
                        continue
                    except Exception, e:
                        self._app.log_exception("Failed to load review data for %s#%d" % path_revision)
                        continue
        
                    if review_data:
                        new_publish_review_data[path_revision] = review_data
    
                else:
                    self._app.log_info("Published file already exists for %s#%d" % path_revision)
                    publish_entities[path_revision] = sg_published_file                
    
            # --------------------------------------------------------------------------------------------
            # --------------------------------------------------------------------------------------------
            # SECOND PASS:
            # - convert all dependency paths into their equivelant Shotgun entities:
            all_dependency_paths = set()
            for info in new_publish_dependencies.values():
                paths = info.get("paths")
                if paths:
                    all_dependency_paths.update(paths)
    
            dependency_publishes = {}
            if all_dependency_paths:
                # get perforce details for the paths at this change:
                p4_paths = dict([("%s@%d" % (p, change_id), p) for p in all_dependency_paths])
                p4_res = p4_fw.util.get_depot_file_details(p4, p4_paths.keys())
        
                # use the revision info retrieved from Perforce to find the
                # Shotgun entities
                for depot_path_key, p4_details in p4_res.iteritems():
                    file_revision = p4_details.get("headRev") if p4_details else None
                    if not file_revision:
                        continue
                    file_revision = int(file_revision)
                    depot_path = p4_paths[depot_path_key]
                    
                    # find the entity from Shotgun:
                    sg_published_file = self.__find_publish_entity(depot_path, file_revision)
                    
                    if sg_published_file:
                        dependency_publishes[depot_path] = sg_published_file
    
            # update the dependency information in Shotgun where needed for the
            # newly created entities:
            pf_entity_type = sgtk.util.get_published_file_entity_type(self._app.sgtk)
            pf_dependency_type = "PublishedFileDependency" if pf_entity_type == "PublishedFile" else "TankDependency"
            sg_batch_requests = []
            
            self._app.log_debug("Updating dependencies...")
            for path_revision, info in new_publish_dependencies.iteritems():
                (depot_path, file_revision) = path_revision
                
                dep_ids = set(info.get("ids", []))
                dep_paths = info.get("paths", [])
                
                # convert dependency paths to ids:
                for dep_path in dep_paths:
                    dep_entity = dependency_publishes.get(dep_path)
                    if not dep_entity:
                        self._app.log_error("Failed to find Shotgun entity for dependency '%s' when processing %s#%d" 
                                            % (dep_path, depot_path, file_revision))
                        continue
                    
                    dep_ids.add(dep_entity["id"])
                    
                if not dep_ids:
                    continue
                    
                # create sg update data:
                publish_entity = publish_entities[path_revision]
                for id in dep_ids:
                    dependent_entity = {"type":pf_entity_type, "id":id}
                                    
                    create_data = None
                    # handle both new and old style published file entity types - shouldn't be needed
                    # but best to do just in case!
                    if pf_entity_type == "PublishedFile":                
                        create_data = {"published_file": publish_entity, 
                                       "dependent_published_file": dependent_entity}
                    else:# pf_entity_type == TankPublishedFile
                        create_data = {"tank_published_file": publish_entity, 
                                       "dependent_tank_published_file": dependent_entity}
                    
                    # add the request to the list to be processed:
                    sg_batch_requests.append({"request_type": "create", 
                                              "entity_type": pf_dependency_type,
                                              "data":create_data})
    
            if sg_batch_requests:
                self._app.log_debug("Creating %d new dependencies in Shotgun..." % len(sg_batch_requests))
                self._app.shotgun.batch(sg_batch_requests)                
    
            # --------------------------------------------------------------------------------------------
            # --------------------------------------------------------------------------------------------
            # THIRD PASS:    
            # - if any review data was found for the new entities then process it:
            if new_publish_review_data:
                
                # first, consolidate data across entities:
                consolidated_review_data = []
                for path_revision, data in new_publish_review_data.iteritems():
    
                    found_entry = None
                    for entry in consolidated_review_data:
                         if entry["data"] == data:
                             found_entry = entry
                             break
                         
                    if not found_entry:
                        # new data so add a new entry:
                        found_entry = {"data":data, "publishes":list()}
                        consolidated_review_data.append(found_entry)
                        
                    found_entry["publishes"].append(publish_entities[path_revision])
    
                # and create new Version entities for each consolidated review data:
                for entry in consolidated_review_data:
                    
                    data = entry["data"]
                    publishes = entry["publishes"]
                    
                    # update data:
                    data["description"] = change_desc # Always use change list description for the comment!
                    data["user"] = sg_user
                    data["created_by"] = sg_user
                    data["created_at"] = change_time
                    
                    uploaded_movie_path = None
                    if "sg_uploaded_movie" in data:
                        uploaded_movie_path = data["sg_uploaded_movie"]
                        del(data["sg_uploaded_movie"])
    
                    if "published_files" in data:
                        del(data["published_files"])
    
                    if pf_entity_type == "PublishedFile":
                        data["published_files"] = publishes
                    else:# == "TankPublishedFile"
                        # the old tank published file link can only handle a single entity!
                        data["tank_published_file"] = publishes[0]
    
                    try:
                        # create the entity:                
                        version_entity = self._app.shotgun.create("Version", data)
        
                        if uploaded_movie_path:
                            # upload the movie:
                            self._app.shotgun.upload("Version", 
                                                       version_entity['id'], 
                                                       uploaded_movie_path, 
                                                       "sg_uploaded_movie" )
                    except Exception, e:
                        self._app.log_error("Failed to create Shotgun Version entity!: %s" % e)
                            
        finally:
            # delete all temp files that were created:
            for path in temporary_files:
                if not os.path.exists(path):
                    continue
                try:
                    os.remove(path)
                except:
                    pass
                      
        return publish_entities.values()

    def __validate_depot_path(self, depot_path, p4):
        """
        Validate that the depot path is a file that Toolkit understands (it's in the same project this
        command is running in and matches a known Toolkit template).  If it does then also attempt to
        construct a context for the specified depot path.
        
        Although the context should ideally be preserved through the publish data when published, we
        still need to handle the case where the file may have been submitted directly through Perforce
        
        :param depot_path:            The depot path to validate
        :param p4:                    The Perforce connection to use
        :returns (Boolean, Context):  True/False if the depot path is a valid Toolkit path, together
                                      with a context created from the path if it is.
        """
        # find the depot root and tk instance for the depot path:
        details = self.__find_file_details(depot_path, p4)
        if not details:
            # the path is not under a Toolkit storage
            return (False, None)
        depot_project_root, tk = details
        
        # check that this tk instance is for the same project we're running in:
        if tk.pipeline_configuration.get_project_id() != self._app.context.project["id"]:
            # it isn't!
            return (False, None)     
        
        # Check all data roots to see if this is recognized by the Toolkit instance.  If no valid 
        # template can be found then Toolkit won't understand the file.
        template = None
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
            return (False, None)
        
        # now try to construct a context from the proxy local path.
        # (TODO) - this is obviously very fragile so need a way to do this using the depot path instead
        # - maybe be able to set the project root and then set it to depot_project_root?
        # - this would also allow template_from_path to work on depot paths...        
        context = self._app.sgtk.context_from_path(proxy_local_path)
                
        # if we don't have a task but do have a step then try to determine the task from the step:
        # (TODO) - this logic should be moved to a hook (probably in core!) as it won't work if 
        # there are Multiple tasks on the same entity that use the same Step!
        if context and not context.task:
            if context.entity and context.step:
                sg_res = self._app.shotgun.find("Task", [["step", "is", context.step], ["entity", "is", context.entity]])
                if sg_res and len(sg_res) == 1:
                    context = self._app.sgtk.context_from_entity(sg_res[0]["type"], sg_res[0]["id"])
        
        return (True, context)        

    def __find_publish_entity(self, depot_path, revision):
        """
        Find the publish entity for a specific revision of a depot path
        
        :param depot_path:    The depot path to check
        :param revision:      The revision to check
        :returns dict:        A Shotgun entity dictionary for a PublishedFile
                              entity if found, otherwise None
        """
        pf_entity_type = sgtk.util.get_published_file_entity_type(self._app.sgtk)
        # (TODO) - improve this filter so that it returns less stuff
        # Unfortunately we can't currently filter on path!
        filters = [["project", "is", self._app.context.project],
                   ["version_number", "is", int(revision)]]
        sg_res = self._app.shotgun.find(pf_entity_type, filters, ["id", "path", "version"])
        sg_published_file = None
        for sg_entity in sg_res:
            url = sg_entity.get("path", {}).get("url")
            if url:
                path_and_version = p4_fw.util.depot_path_from_url(url)
                if path_and_version and path_and_version[0] == depot_path:
                    # found it!
                    return {"type":sg_entity["type"], "id":sg_entity["id"]}

    def __find_file_details(self, depot_path, p4):
        """
        Find the depot project root and tk instance for the specified depot path
        if possible.
        
        :param depot_path:        Depot path to check
        :param p4:                Perforce connection to use
        :returns (str, Sgtk):     Tuple containing (depot project root, sgtk instance)
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

    def __connect_to_perforce(self):
        """
        Connect to Perforce
        
        :returns P4:    A connected Perforce instance if successful
        """
        try:
            p4 = p4_fw.connection.connect(False, self.__p4_user, self.__p4_pass)
            return p4
        except:
            self._app.log_exception("Failed to connect!")
            return None
        
    def __get_sg_user(self, perforce_user):
        """
        Get the Shotgun user for the specified Perforce user
        
        :param perforce_user:    The Perforce user to find the corresponding Shotgun user for
        :returns dict:           A Shotgun entity dictionary for the Shotgun user if found
        """
        return p4_fw.get_shotgun_user(perforce_user)
        
    def __get_depot_project_root(self, depot_path, p4):
        """
        Find the depot-relative project root for the specified depot file
        
        :param depot_path:    The depot path to find the project root for
        :param p4:            The Perforce connection to use
        :returns str:         The depot-relative project root
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
            
            tank_configs_path = "%s/%s" % (project_root, ShotgunSync.CONFIG_BACK_MAPPING_FILE_LOCATION)
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
        Determine the local pipeline configuration directory for the given depot project_root
        
        :param project_root:    The depot relative project root
        :param p4:              The Perforce connection to use
        :returns str:           The local pipeline configuration root directory
        """
        # first, check to see if info is in cache:
        pc_root = self.__project_pc_roots.get(project_root)
        if pc_root != None:
            return pc_root
        self.__project_pc_roots[project_root] = ""
        
        # check that the tank_configs.yml file is in the correct place:
        tank_configs_path = "%s/%s" % (project_root, ShotgunSync.CONFIG_BACK_MAPPING_FILE_LOCATION)        
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
            