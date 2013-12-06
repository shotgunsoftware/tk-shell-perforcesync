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

import time
import sgtk
from P4 import P4Exception

from .shotgun_sync import ShotgunSync

class ShotgunSyncDaemon(object):
    """
    """
    P4_COUNTER_BASE_NAME = "tk_perforcesync_project_"
    
    def __init__(self, app):
        """
        """
        self._app = app
        self._interval = self._app.get_setting("poll_interval")
        self._p4_counter_name = "%s%d" % (ShotgunSyncDaemon.P4_COUNTER_BASE_NAME, self._app.context.project["id"])
        
        self._p4_sync = ShotgunSync(self._app)
        
    def run(self):
        """
        Run continuous daemon!
        """
        while True:
            
            self._app.log_info("Checking for new Perforce changes to sync with Shotgun...")
            
            # open connection to perforce:
            p4 = self._connect_to_perforce()
            if p4:            
                try:
                    # look to see if there are new changes to process:
                    change_range = self._get_new_changes(p4)
                    
                    if change_range:
                        # process change range:
                        self._process_changes(change_range[0], change_range[1], p4)
                        
                        # update counter:
                        self._update_perforce_counter(change_range[1], p4)
                finally:
                    p4.disconnect()
            else:
                self._app.log_error("Failed to open connection to Perforce server!")                    
                        
            # and sleep for a bit:
            self._app.log_debug("Sleeping for %d seconds" % self._interval)
            time.sleep(self._interval)
    
    def _connect_to_perforce(self):
        """
        """
        try:
            p4_fw = sgtk.platform.get_framework("tk-framework-perforce")
            p4 = p4_fw.connect(False)
            return p4
        except:
            self._app.log_exception("Failed to connect!")
            return None    
    
    def _get_new_changes(self, p4):
        """
        """
        try:
            # query start change from perforce counter:
            p4_res = p4.run_counter(self._p4_counter_name)
            # [{'counter': 'tk_shotgun_sync', 'value': '0'}]
            start_change = int(p4_res[0]["value"])+1 if p4_res else 0
            
            # get highest submitted change from perforce:
            p4_res = p4.run_changes("-m", "1", "-s", "submitted")
            # [{'status': 'submitted', 'changeType': 'public', 'change': '36', ...}]
            end_change =  int(p4_res[0]["change"])
            
            if end_change >= start_change:
                return (start_change, end_change)
            
        except P4Exception, e:
            self._app.log_error("Failed to determine changelist range: %s" % p4.errors[0] if p4.errors else e)
        except Exception, e:
            self._app.log_error("Failed to determine changelist range: %s" % e)        
    
    def _process_changes(self, start_change, end_change, p4):
        """
        """
        # sync changes:
        for change_id in range(start_change, end_change + 1):
            try:
                self._p4_sync.sync_change(change_id, p4)
                #self._sync_change(change_id, p4)
            except Exception, e:
                self._app.log_error("Failed to sync change %d: %s" % (change_id, e))
    
    def _update_perforce_counter(self, change, p4):
        """
        """
        # update counter:                               
        try:           
            p4.run_counter(self._p4_counter_name, str(change))
        except P4Exception, e:
            self._app.log_error("Failed to update Perforce counter '%s' - %s" 
                           % (self._p4_counter_name, (p4.errors[0] if p4.errors else e)))
        except Exception, e:
            self._app.log_error("Failed to update Perforce counter '%s' - %s" % (self._p4_counter_name, e))