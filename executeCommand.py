# -*- coding: utf-8 -*-
"""
Created on Sun May  1 16:48:16 2022

@author: ribal
"""

from subprocess import call, PIPE, run

machineManagerDir = "Documents/Github/kollector-worker"

def getProperties():
    retcode = run(['osascript', '-e', 'tell application "Terminal" to get {properties, properties of tab 1} of window 1'], stdout=PIPE)
    retcode = retcode.stdout.decode('utf-8')
    # retcode = "tab 1 of window xxx, ..."
    pid = retcode.split(',')[0][-3:].strip()
    print("window id:", pid)
    return pid

def turnOnWorker(configFilePath):
    oascriptCommand = 'tell application "Terminal"\n\n\tdo script "conda activate workerenv && cd {managingDir} && python worker.py {configPath}"\nend tell'.format(managingDir=machineManagerDir, configPath=configFilePath)
    retcode = call(['osascript', '-e', oascriptCommand])
    return retcode

def turnOffAllButManager():
    exitCode = call("./closeAll.sh", shell=True)
    return exitCode

def turnOffWorker(name):
    exitCode = call("./killWorker.sh %s" % name, shell=True)
    with open("./closeWorkerWindow.txt", "r") as script:
        appleScript = script.read() % name
        print(appleScript)
        exitCode = call(['osascript', '-e', appleScript])
    return exitCode

def updateCode(gitRepo):
    exitCode = call("./pullAndRestart.sh %s" % gitRepo, shell=True)
    return exitCode

def updateRequirements():
    exitCode = call("./updateRequirements.sh", shell=True)
    return exitCode