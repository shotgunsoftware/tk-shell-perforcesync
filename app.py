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
Application to handle syncing of Perforce submitted changes with Shotgun.
"""

import os
import optparse
    
import sgtk
from sgtk import TankError

class PerforceSync(sgtk.platform.Application):
    """
    Application class
    """

    def init_app(self):
        """
        Called as the application is being initialized
        """
        self.log_debug("%s: Initializing..." % self)
        
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
        
    def destroy_app(self):
        """
        Called when app is destroyed
        """
        self.log_debug("%s: Destroying..." % self)        
        
    class SyncOptionParser(optparse.OptionParser):
        """
        Custom version of the optparse.OptionParser that doesn't
        exit when an error is encountered, instead raising a
        TankError that can be handled as normal
        """
        def error(self, msg):
            raise TankError(msg)

        def exit(self, status=0, msg=None):
            pass
        
    def sync_changes(self, *args):
        """
        Sync the specified Perforce change with Shotgun
        
        :param args:    Arguments passed through the shell command line
        """
        
        # Parse args to extract the command arguments.
        # (TODO) - this could potentially be moved up to the shell engine
        # in a more generic fashion at some point so that the arguments can
        # just be specified in the function directly.
        parser = PerforceSync.SyncOptionParser()
        parser.add_option("-s", "--start", help="Start change to sync", type="int")
        parser.add_option("-e", "--end", help="End change to sync (optional)", type="int")
        parser.add_option("-u", "--username", help="Username to use to log-in to Perforce (optional)", type="str")
        parser.add_option("-p", "--password", help="Password to use to log-in to Perforce (optional)", type="str")
        
        start_change = end_change = None
        p4_user = p4_pass = None
        try:
            options, _ = parser.parse_args(list(args))
            start_change = options.start
            end_change = options.end
            p4_user = options.username
            p4_pass = options.password
        except TankError, e:
            self.log_error("Failed to parse command arguments - %s" % e)
            return

        # validate arguments:
        if start_change is None:
            self.log_error("Must specify at least a start change to sync!")
            return
        
        if end_change is None:
            end_change = start_change
        elif end_change < start_change:
            end_change = start_change
            self.log_warning("End change is before start change - ignoring!")
            
        end_change = max(start_change, end_change) if end_change is not None else start_change
        
        # sync changes:
        tk_shell_perforcesync = self.import_module("tk_shell_perforcesync")
        sync_handler = tk_shell_perforcesync.ShotgunSync(self, p4_user, p4_pass)
        sync_handler.sync_changes(start_change, end_change)
        
    def sync_changes_daemon(self, *args):
        """
        Run the Perforce sync continuously
        
        :param args:    Arguments passed through the shell command line
        """
        # Parse args to extract the command arguments.
        # (TODO) - this could potentially be moved up to the shell engine
        # in a more generic fashion at some point so that the arguments can
        # just be specified in the function directly.
        parser = PerforceSync.SyncOptionParser()
        parser.add_option("-s", "--start", help="Start change to sync (optional)", type="int")
        parser.add_option("-u", "--username", help="Username to use to log-in to Perforce (optional)", type="str")
        parser.add_option("-p", "--password", help="Password to use to log-in to Perforce (optional)", type="str")        
        
        start_change = None
        p4_user = p4_pass = None
        try:
            options, _ = parser.parse_args(list(args))
            start_change = options.start
            p4_user = options.username
            p4_pass = options.password
        except TankError, e:
            self.log_error("Failed to parse command arguments - %s" % e)
            return        
        
        tk_shell_perforcesync = self.import_module("tk_shell_perforcesync")
        daemon = tk_shell_perforcesync.ShotgunSyncDaemon(self, start_change, p4_user, p4_pass)
        daemon.run()
    
    

        