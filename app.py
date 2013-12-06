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
import sgtk
from sgtk import TankError

class PerforceSync(sgtk.platform.Application):

    def init_app(self):
        """
        Called as the application is being initialized
        """
        #self.engine.register_command("Sync", self.sync_changes)
        
        # register commands:
        #
        
        # single-shot sync command:
        params = {"short_name": "sync_perforce", 
                  "title": "Sync Perforce Change",
                  "description": "Sync specified Perforce change(s) with Shotgun"}
        self.engine.register_command(params["title"], 
                                     self.sync_changes, 
                                     params)
        
        # run sync as daemon:
        params = {"short_name": "sync_perforce_daemon", 
                  "title": "Run Perforce Sync Daemon",
                  "description": "Run daemon to sync Perforce changes with Shotgun"}
        self.engine.register_command(params["title"], 
                                     self.sync_changes_daemon, 
                                     params)
        
    def sync_changes(self, change_str):
        """
        """
        # parse change_str to get change range to sync:
        # ...
        
        # sync changes:
        tk_shell_perforcesync = self.import_module("tk_shell_perforcesync")
        sync_handler = tk_shell_perforcesync.ShotgunSync(self)
        sync_handler.sync_changes(0, 0)
        
    def sync_changes_daemon(self):
        """
        """
        tk_shell_perforcesync = self.import_module("tk_shell_perforcesync")
        daemon = tk_shell_perforcesync.ShotgunSyncDaemon(self)
        daemon.run()
    
    
    def destroy_app(self):
        self.log_debug("Destroying tk-shell-shotgunsync")