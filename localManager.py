# -*- coding: utf-8 -*-
"""
Created on Sat Apr 30 21:13:10 2022

@author: ribal
"""

import threading
from myWorkers import MyWorkers
import socket
from executeCommand import updateCode, updateRequirements
import firebase_admin
from firebase_admin import credentials, firestore

MACHINE_NAME = socket.gethostname()
JSON_CERTIFICATE = "spherical-list-284723-95ce4d8207b4.json"
WORKERS_COLLECTION_NAME = "workers"
MANAGER_COLLECTION_NAME = "worker_managers"
WORKERS_PATH = "workers/"

if not firebase_admin._apps:
    credentials = credentials.Certificate(JSON_CERTIFICATE)
    defaultApp = firebase_admin.initialize_app(credentials)

db = firestore.client()

stop = False

def startManager():
    listenDocument(MACHINE_NAME)
    while not stop:
        continue
    
    
def listenDocument(machine):
    # Create an Event for notifying main thread.
    callback_done = threading.Event()
    
    # Create a callback on_snapshot function to capture changes
    def on_snapshot(doc_snapshot, changes, read_time):
        for doc in doc_snapshot:
            print(f'Received document snapshot: {doc.id}')
            for change in changes:
                print(change.type.name)
            analyseChange(doc)
        callback_done.set()
    
    doc_ref = db.collection(MANAGER_COLLECTION_NAME).where("machine", "==", machine)
    
    # Watch the document
    doc_watch = doc_ref.on_snapshot(on_snapshot)
    
def analyseChange(doc):
    # read todo from firestore
    docDict = doc.to_dict()
    todo = docDict["todo"]
    
    # clear read commands on firestore
    docDict["todo"] = []
    db.collection(MANAGER_COLLECTION_NAME).document(doc.id).update({"todo":{}})
    
    for command, arg in todo.items():
        print(command, arg)
        if command == "turn_on":
            if arg == "all":
                myWorkers.turnOnAllWorkers()
            else:
                emailList = arg.split(' ') # split by white space
                for email in emailList:
                    myWorkers.turnOn(email)
        elif command == "turn_off":
            if arg == "all":
                myWorkers.turnOffAll()
            else:
                emailList = arg.split(' ') # split by white space
                for email in emailList:
                    myWorkers.turnOff(email)
        elif command == "add":
            addWorkerOnFirestore(arg)
        elif command == "remove":
            removeWorkerOnFirestore(arg)
        elif command == "git_pull":
            updateCode(arg)
            stop = True
        elif command == "update_requirements":
            updateRequirements()
            stop = True
        else:
            print("Command not found: '%s'"  %command)
    
def addWorkerOnFirestore(email):
    myWorkers.addWorker(email)
    docs = db.collection(MANAGER_COLLECTION_NAME).where("machine", "==", MACHINE_NAME).get()
    for doc in docs:
        updatedDoc = doc.to_dict()
        updatedDoc["current_workers"].append({u"email":email, u"status": u"off"})      
        db.collection(MANAGER_COLLECTION_NAME).document(doc.id).set(updatedDoc) 

def removeWorkerOnFirestore(email):
    myWorkers.removeWorker(email)
    docs = db.collection(MANAGER_COLLECTION_NAME).where("machine", "==", MACHINE_NAME).get()
    for doc in docs:
        updatedDoc = doc.to_dict()
        currentWorkers = updatedDoc["current_workers"]
        for worker in currentWorkers:
            if email in worker.values():
                currentWorkers.remove(worker)
                
        updatedDoc["current_workers"] = currentWorkers        
        db.collection(MANAGER_COLLECTION_NAME).document(doc.id).set(updatedDoc)
            
def getWorkersFromFirestore():
    managerDoc = db.collection(MANAGER_COLLECTION_NAME).where("machine", "==", MACHINE_NAME).get()
    currentWorkers = []
    for doc in managerDoc:
        currentWorkers = doc.to_dict()["current_workers"]
    workersEmails = []
    for worker in currentWorkers:
        workersEmails.append(worker["email"])
    return workersEmails

def updateWorkerStatusFirestore(worker):
    docs = db.collection(MANAGER_COLLECTION_NAME).where("machine", "==", MACHINE_NAME).get()
    for doc in docs:
        updatedDoc = doc.to_dict()
        updated_current_workers = []
        for current_worker in updatedDoc["current_workers"]:
            if worker.email == current_worker["email"]:
                current_worker["status"] = worker.status # update current worker status
            updated_current_workers.append(current_worker)
        
        updatedDoc["current_workers"] = updated_current_workers
        db.collection(MANAGER_COLLECTION_NAME).document(doc.id).set(updatedDoc) 
        
if __name__=="__main__":
    print(MACHINE_NAME)
    myWorkers = MyWorkers()
    myWorkers.setupWorkers(getWorkersFromFirestore())
    startManager()
    