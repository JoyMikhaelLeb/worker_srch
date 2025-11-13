# -*- coding: utf-8 -*-
"""
Created on Tue May  3 13:19:48 2022

@author: ribal
"""
import os
import time
import shutil
import json
from executeCommand import turnOnWorker, turnOffAllButManager, turnOffWorker

WORKERS_PATH = "workers/"
JSON_CERTIFICATE = "spherical-list-284723-95ce4d8207b4.json"

configDict = {
  "log_level": "debug",
  "firestore_certificate": "spherical-list-284723-216944ab15f1.json",
  "batch_size": 3,
  "headless_chrome": False
}

class Worker:
    def __init__(self, email):
        self.email = email
        self.name = email.split("@")[0]
        self.configFilePath = WORKERS_PATH +"{}_config.json".format(self.name)
        self.status = "off"
    
    def turnOn(self):
        """
        Turn on worker by launching the worker script with the corresponding config path

        Returns
        -------
        bool
            True if worker is successufully turned on, false otherwise.
        """
        if self.isActive():
            print(self.email, "is already logged in")
            return False
    
        try:
            print("\n\nturning on worker:", self.email)
            retcode = turnOnWorker(self.configFilePath)
            time.sleep(1)
            if retcode == 0:             
                self.status = "on"
                return True
            else:
                print("Failed to turn on worker, return code:", retcode)
                return False
        except Exception as e:
            print("Failed to turn on worker error:", e)
            return False
    
    def turnOff(self):
        if not self.isActive():
            print(self.email, "is not logged in")
            return False
        try:
            print("\n\nturning off worker:", self.email)
            retcode = turnOffWorker(self.name)
            if retcode == 0:
                self.status = "off"
                return True
            else:
                print("Failed to turn off worker, return code:", retcode)
                return False
        except Exception as e:
            print("Failed to turn off worker error:", e)
            return False
        
    def createConfigFile(self):
        try:
            with open(self.configFilePath, "w+") as configFile:
                configDict["email"] = self.email
                json.dump(configDict, configFile)
            return True
        except:
            return False
    
    def isActive(self):
        return True if self.status == "on" else False
    
    def to_dict(self):
        return {"email": self.email, "status": self.status}
    
    def __str__(self):
        return "{email: " + self.email + ",\nname: " + self.name + ",\nconfig: " + self.configFilePath + ",\nstatus: " + self.status + "}"
    
class MyWorkers:
    def __init__(self):
        self.workers = {} #{workerEmail:<Class Worker>, workerEmail2:<Class Worker>}
    
    def turnOn(self, email):
        if email not in self.workers.keys():
            print("Worker %s does not exist" % email)
            return False
        worker = self.workers[email]
        worker.turnOn()
        
    def turnOnAllWorkers(self):
        """
        Turns on all INACTIVE workers
        """
        for worker in self.workers.values():
            worker.turnOn()
            
    def turnOff(self, email):
        if email not in self.workers.keys():
            print("Worker %s does not exist" % email)
            return False
        worker = self.workers[email]
        worker.turnOff()

    def turnOffAll(self):
        print("turning off all workers...")
        retcode = turnOffAllButManager()
        if retcode == 0:
            for worker in self.workers.values():
                worker.status = "off"
            
    def addWorker(self, email):
        if email in self.workers.keys():
            print("Worker %s already exists" % email)
            return False
        # first create worker
        newWorker = Worker(email)
        newWorker.createConfigFile()
        self.workers[newWorker.email] = newWorker
        
    def removeWorker(self, email):
        if email not in self.workers.keys():
            print("Worker %s does not exist" % email)
            return False
        worker = self.workers[email]
        if worker.isActive():
            print("Could not remove worker %s: turn off before removing" % email)
            return False
        if worker == False:
            print("Worker does not exist")
            return False
        try:
            os.remove(worker.configFilePath)
            self.workers.pop(worker.email)
        except FileNotFoundError:
            print("config file does not exist for worker: %s" % worker.name)
    
        
    def setupWorkers(self, workersEmails):
        """
        Deletes all workers config files and recreates them based on what is already 
        under "current_workers" in firestore

        Parameters
        ----------
        workersEmails : List<STR>
        """
        # delete existing workers (TODO: make sure we need this step)
        try:
            shutil.rmtree(WORKERS_PATH)
        except Exception as e:
            print("Handled error: %s : %s" % (WORKERS_PATH, e.strerror))
        os.makedirs(WORKERS_PATH)
        for email in workersEmails:
            self.addWorker(email)
    
    def getActiveWorkers(self):
        """
        TODO:
        Will get this info from firestore later: to be implemented
        """
        activeWorkers = {}
        for email, worker in self.workers.items():
            if worker.isActive():
                activeWorkers[email] = worker
        return activeWorkers
        
        
