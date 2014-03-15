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
Daemon functionality for the Shotgun sync process.  Runs the sync in a loop at a configured
time interval
"""

import time

import sgtk
from sgtk import TankError

p4_fw = sgtk.platform.get_framework("tk-framework-perforce")
from P4 import P4Exception

from .shotgun_sync import ShotgunSync

class ShotgunSyncDaemon(object):
    """
    Class to encapsulate the daemon behaviour to sync Perforce changes with Shotgun by 
    iterating through new changes as they are submitted. 
    """
    P4_COUNTER_BASE_NAME = "tk_perforcesync_project_"
    
    def __init__(self, app, start_change=None, p4_user=None, p4_pass=None):
        """
        Construction
        
        :param app:            The app bundle that constucted this object
        :param start_change:   The first change to consider syncing - this provides
                               an easy way to skip a large number of changes that
                               are known not to contain Toolkit data
        :param p4_user:        The Perforce user that the command should be run under
        :param p4_pass:        The Perforce password that the command should be run under
        """
        self.__app = app
        self.__start_change = start_change
        self.__p4_user = p4_user
        self.__p4_pass = p4_pass
        
        self._interval = self.__app.get_setting("poll_interval")
        
        self._p4_counter_name = "%s%d" % (ShotgunSyncDaemon.P4_COUNTER_BASE_NAME, self.__app.context.project["id"])
        self._p4_sync = ShotgunSync(self.__app, self.__p4_user, self.__p4_pass)
        
    def run(self):
        """
        Run continuous daemon
        """
        start_change = self.__start_change
        while True:
            self.__app.log_info("Checking for new Perforce changes to sync with Shotgun...")

            p4 = None
            try:
                # re-connect
                p4 = p4_fw.connection.connect(False, self.__p4_user, self.__p4_pass)
            except TankError, e:
                self.__app.log_error("Failed to connect to Perforce server: %s" % e)
            else:
                res = 1
                while res:
                    res = self.__process_next_change(p4, start_change)
                    if isinstance(res, int):
                        # processed a change so move to the next one:
                        start_change = res+1
                    else:
                        # didn't process anything
                        break
            finally:
                p4.disconnect()

            # didn't do anything so sleep for a bit:
            self.__app.log_debug("No new changes found - sleeping for %d seconds" % self._interval)
            time.sleep(self._interval)

    def __process_next_change(self, p4, start_change=0):
        """
        Attempt to register a new 'Revision' entity in Shotgun for the next 
        submitted Perforce change that needs to be processed.

        Because neither Perforce nor Shotgun allow this to happen in an atomic way, we
        use both to ensure that a change is only processed by a single daemon.
        
        :param p4:              The Perforce connection object to use
        :param start_change:    Start looking for the next submitted change from this is or the value of
                                the Perforce counter, whichever is highest.
        """
        # get the current counter value
        p4_counter = self.__retrieve_counter(p4)
        
        # Get the next submitted change starting from either the counter+1 or the start
        # change, whichever is highest.        
        p4_change = self.__find_next_submitted_change(p4, max(start_change, p4_counter+1))
        if not p4_change:
            return

        change_id = int(p4_change["change"])
        
        # validate that this change is in fact in this project:
        if not self._p4_sync.is_change_in_context(p4, p4_change):
            # nothing to do so skip
            return change_id
        
        # next, create this change in Shotgun in an atomic way:
        sg_change_entity = self._p4_sync.create_sg_entity_for_change(p4_change)
        if sg_change_entity:
            # As we were successful, update Perforce to tell it we 
            # have processed this change.  This only happens if this process
            # has correctly created a new Revision entity for this change in
            # Shotgun.
            self.__update_counter(p4, change_id)
        
            # finally, process the change contents:
            self._p4_sync.sync_change_contents(p4, p4_change, sg_change_entity)
        
        return change_id
    
    def __find_next_submitted_change(self, p4, start_change):
        """
        Find the next submitted change from Perforce with a change id >= start_change.
        
        :param p4:              The Perforce connection to use
        :param start_change:    Minimum change to look for new changes from
        """
        self.__app.log_debug("Looking for the next change submitted to Perforce...")        
        try:
            # get highest submitted change from perforce:
            # returns: [{'status': 'submitted', 'changeType': 'public', 'change': '36', ...}]            
            p4_res = p4.run_changes("-m", "1", "-s", "submitted")
            if not p4_res:
                # nothing submitted!
                return
            end_change =  int(p4_res[0]["change"])
            if end_change < start_change:
                # nothing new submitted!
                return
            
            # Find the next submitted change, skipping any pending or shelved changes:
            # Perforce always increments the submitted change id even if there are previous
            # shelved or pending changes so it's safe to assume that they are sequentially 
            # ordered
            #
            # (TODO) - is there an easier way to determine this?
            block_size = 10
            for block_start in range(start_change, end_change+1, block_size):
                block_end = min(block_start + block_size - 1, end_change)
                
                p4_res = p4.run_describe(range(block_start, block_end + 1))
                changes_by_change = dict([(r["change"], r) for r in p4_res if "change" in r])
                
                for change_num in range(block_start, block_end + 1):
                    change = changes_by_change.get(str(change_num))
                    if not change or change.get("status") != "submitted":
                        continue
                
                    # this is the next submitted change
                    return change
            
        except P4Exception, e:
            self.__app.log_error("Failed to find next change to process: %s" % p4.errors[0] if p4.errors else e)
        except Exception, e:
            self.__app.log_error("Failed to find next change to process: %s" % e)          
    
    def __retrieve_counter(self, p4):
        """
        Retrieve the perforce counter for this project
        
        :param p4:    The Perforce connection to use
        """
        try:
            p4_res = p4.run_counter(self._p4_counter_name)
            return int(p4_res[0]["value"]) if p4_res else 0            
        except P4Exception, e:
            self.__app.log_error("Failed to retrieve Perforce counter '%s' - %s" 
                           % (self._p4_counter_name, (p4.errors[0] if p4.errors else e)))
        except Exception, e:
            self.__app.log_error("Failed to retrieve Perforce counter '%s' - %s" % (self._p4_counter_name, e))                    
    
    def __update_counter(self, p4, change_id):
        """
        Update the perforce counter to the specified change id.
        
        :param p4:         The perforce connection to use
        :param change_id:  The change id to update the counter to
        """ 
        self.__app.log_debug("Updating the Perforce counter '%s' to %s" % (self._p4_counter_name, change_id))                        
        try:
            p4.run_counter(self._p4_counter_name, str(change_id))
        except P4Exception, e:
            self.__app.log_error("Failed to update Perforce counter '%s' - %s" 
                           % (self._p4_counter_name, (p4.errors[0] if p4.errors else e)))
        except Exception, e:
            self.__app.log_error("Failed to update Perforce counter '%s' - %s" % (self._p4_counter_name, e))
            
            
            