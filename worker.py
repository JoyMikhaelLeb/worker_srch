#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 12 16:46:48 2022

@author: charb,Joy
"""
import re
from save_website import extract_domain
from time import sleep, time
import signal
from getnumericals import getNumerical
# from supabase_founding_dates_interface import connect_to_supabase
from supabase_founding_dates_interface import update_person,replace_linkedin_handle,update_oldest_founder_founding_if_older_add_about,Raw_Entity

from abouts import getAbouts
from ppl_info_old  import getppl

from search_ppl import search_position
from advanced_search_ppl import sales_search
from advanced_search_ppl import get_advanced_search_people_profile
from advanced_search_companies import sales_company_search
from advanced_search_companies import get_advanced_search_company_profile
from downloadhtml import get_profile_sales_html
#adv is based on the one will import now
from advanced_profiles_html import advanced_search_people_profile_html
from insights import get_25_months_employees
from utils import linkedin_login, linkedin_login_nodriver_sync, getVerificationCode,linkedin_logout, getPass, get_numericalID, get_name_and_numericalID,getLink
import logging
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

import os
from sn_employees_movements import get_sn_employees_movements
import datetime
from datetime import date
from datetime import datetime as datetime_class
import sys
import traceback
import json

from random import randint
# Removed selenium imports - now using nodriver
from urllib3.connectionpool import log as urllibLogger
urllibLogger.setLevel(logging.FATAL)

class CustomFormatter(logging.Formatter):

    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    # format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"

    format = "%(asctime)s\t[%(levelname)s]\t%(message)s"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

class Worker:
    """
    Class designed to manage workers

    Parameters:
    config : str
        Path to worker configuration file.
    """

    def __init__(self, config: str):

        self.logger = None

        with open(config, "r", encoding="utf8") as config_file:
            config = json.load(config_file)

        self.log_level = config["log_level"]

        self.headless = config["headless_chrome"]

        if not firebase_admin._apps:
            cred = credentials.Certificate(config["firestore_certificate"])
            firebase_admin.initialize_app(cred)

        self.db = firestore.client()

        self.email = config["email"]
        self.batch_size = config["batch_size"]

        self.__init_logger(config["log_level"])

        self.passwd = getPass(self.email)
        self.verifCode = getVerificationCode(self.email)

        self.driver = None
        self.logged_in = False

        self.worker_ref = (
            self.db.collection("dashboards")
            .document("logs")
            .collection("workers")
            .document(self.email)
        )
        self.tasks_ref = self.db.collection_group("tasks")
        self.entities_ref = self.db.collection("entities")
        self.ppl_ref = self.db.collection("ppl")
        self.ppl_search_ref = self.db.collection("ppl_search")
        self.advanced_ppl_search_ref = self.db.collection("ppl_search_advanced")
        self.advanced_company_search_ref = self.db.collection("companies_search_advanced")
        self.entities_all_employees_history = self.db.collection("entities_all_employees_history")
        self.requests_ref = (
            self.db.collection("automation").document("current").collection("requests")
        )

        self.tasks = []
        self.current_task_id = str()
        self.current_task = None

    def __init_logger(self, log_level: str):
        """
        Initializes logger instance.

        Parameters
        ----------
        log_level : str
            DESCRIPTION.

        Returns
        -------
        None.

        """
        if log_level == "info":
            log_level = logging.INFO
            self.log_level = log_level
        elif log_level == "debug":
            log_level = logging.DEBUG
            self.log_level = log_level
        elif log_level == "warning":
            log_level = logging.WARN
            self.log_level = log_level

        s_handler = logging.StreamHandler(sys.stdout)
        s_handler.setLevel(log_level)

        # log_format = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        s_handler.setFormatter(CustomFormatter())

        self.logger = logging.getLogger(f"worker-{self.email}")

        if self.logger.hasHandlers():
            self.logger.handlers.clear()

        self.logger.addHandler(s_handler)
        self.logger.setLevel(log_level)
        self.logger.propagate = False

    def __login(self) -> bool:
        """
        Attempts worker login into LinkedIn. Also checks if user needs
        validation.

        Returns
        -------
        bool
            True if operation is successful, False otherwise.

        """
        if self.logged_in:
            self.logger.warning(f"{self.email} is already logged in")
            return True

        self.logger.debug(f"Attempting log in of user {self.email}")

        # Use nodriver instead of Selenium
        self.driver = linkedin_login_nodriver_sync(self.email, self.passwd, self.headless)

        if self.__needs_validation_check():
            self.logger.critical(f"User {self.email} needs validation. Logging out.")
            self.logger.debug(f"Updating {self.email} status to needs validation")

            self.__update_worker_status("needs validation")
            self.logged_in = False

            try:
                self.logger.debug(f"Attempting log out of user {self.email}")
                linkedin_logout(self.driver)
            except:
                pass

            finally:
                self.logger.info(f"User {self.email} is logged out.")
                return False

        self.__update_worker_status("online")
        self.logged_in = True
        self.logger.info(f"User {self.email} is logged in.")

        return True

    def __logout(self):
        """
        Attempts worker logout.

        Returns
        -------
        bool
            True if operation is successful, False otherwise.
        """

        if not self.logged_in:
            self.logger.warning(f"{self.email} is already logged out")
            return True
        try:
            self.logger.debug(f"Attempting log out of user {self.email}")
            # For nodriver, we need to close the browser properly
            import asyncio

            async def close_browser():
                try:
                    if hasattr(self.driver, 'browser'):
                        await self.driver.browser.stop()
                    elif hasattr(self.driver, 'close'):
                        await self.driver.close()
                except:
                    pass

            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            loop.run_until_complete(close_browser())
        except:
            pass

        finally:
            self.logger.info(f"User {self.email} is logged out.")
            self.logged_in = False

            self.__update_worker_status("offline")
            return True

    def init_status_controller(self, command: str) -> bool:
        """
        Logs worker in or out based on command.

        Parameters
        ----------
        command : str
            Can be login or logout.

        Returns
        -------
        bool
            True if operation is successful, False otherwise.

        """
        if command.lower() == "login":
            return self.__login()

        elif command.lower() == "logout":
            return self.__logout()

    def __needs_validation_check(self):
        """
        Checks whether the Linkedin account needs validation.

        Returns
        -------
        bool
            True if the user needs validation, False if not.

        """
        try:
            # For nodriver, use async operation
            import asyncio

            async def check_validation():
                try:
                    element = await self.driver.find("main.app__content h1:text('do a quick verification')", timeout=2)
                    return element is not None
                except:
                    return False

            try:
                loop = asyncio.get_event_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            return loop.run_until_complete(check_validation())
        except:
            return False

    def __num_of_tasks_remaining(self) -> int:
        """
        Calculates the number of tasks left for worker

        Returns
        -------
        int
            Number of tasks left.

        """
        try:
            return len([x for x in self.worker_ref.collection("worker_tasks").stream()])
        except:
            return 0

    def __update_worker_status(
        self,
        status: str = None,
        current_request: str = "NA",
        current_task: str = "NA",
    ):
        """
        Updates worker status in firestore.

        Parameters
        ----------
        status : str, optional
            Status of the worker right now. If set to None, status is not updated.
            Status can be:
                online
                offline
                needs validation
            The default is None.
        current_request : str, optional
            Last request id the worker was completing. If NA, it is not updated.
            The default is "NA".
        current_task : str, optional
            Last task id the worker was completing. If NA, it is not updated.
            The default is "NA".
        Returns
        -------
        None.

        """
        if status is not None:
            process = True
            if current_request == "NA" or current_task == "NA":
                doc = {
                    "id": self.email,
                    "status": status,
                    "since": datetime.datetime.utcnow(),
                }

            else:
                doc = {
                    "current_request_processing": current_request,
                    "current_task_processing": current_task,
                    "id": self.email,
                    "status": status,
                    "since": datetime.datetime.utcnow(),
                }

        else:
            if not current_request == "NA" or not current_task == "NA":
                doc = {
                    "current_request_processing": current_request,
                    "current_task_processing": current_task,
                    "id": self.email,
                }
                process = True
            else:
                process = False

        if process:
            self.db.collection("dashboards").document("logs").collection(
                "workers"
            ).document(self.email).set(doc, merge=True)

            if status is not None:
                self.logger.debug(
                    f"{self.email} status updated to {status} in firestore"
                )

    def __check_for_new_tasks(self, get_all=False):
        """
        Fetches the current user tasks and updates the number of tasks in firestore

        Parameters
        ----------
        get_all : bool, optional
            If set to True, method will fetch all tasks from firestore.
            Otherwise, method will fetch defined batch_size.
            Defaults to False.

        Returns
        -------
        int
            Number of tasks fetched.

        """
        if not get_all:
            self.tasks = [
                x.to_dict()
                for x in self.worker_ref.collection("worker_tasks")
                .order_by("t_priority")
                .order_by("updated")
                .limit(self.batch_size)
                .stream()
            ]
            num_of_tasks = self.__num_of_tasks_remaining()
            if num_of_tasks != self.worker_ref.get().to_dict()["task_counter"]:
                self.worker_ref.update({"task_counter": num_of_tasks})
        else:
            self.tasks = [
                x.to_dict()
                for x in self.worker_ref.collection("worker_tasks")
                .order_by("t_priority")
                .order_by("updated")
                .stream()
            ]
        num_of_tasks = self.__num_of_tasks_remaining()
        if num_of_tasks != self.worker_ref.get().to_dict()["task_counter"]:
            self.worker_ref.update({"task_counter": num_of_tasks})

        return len(self.tasks)

    def __remove_current_task_from_firestore_queue(self):
        """
        Deletes the current task from firestore worker_tasks collection

        Returns
        -------
        None.

        """
        self.worker_ref.collection("worker_tasks").document(
            self.current_task_id
        ).delete()

    def __remove_all_remaining_tasks_in_queue(self):
        """
        Deletes the all remaining tasks from firestore worker_tasks collection
        and updates status to in_queue

        Returns
        -------
        None.

        """
        self.__check_for_new_tasks(get_all=True)

        for task in self.tasks:
            self.current_task_id = task["id"]
            if task["category"] == "command":
                self.__remove_current_task_from_firestore_queue()
                continue

            self.__update_current_task_status("in_queue")
            self.__remove_current_task_from_firestore_queue()

    def __update_sb_status_field(
       self, sb_status: str = ""
   ) -> bool:
       """
       Updates sb_status field of the current task
       Parameters
       ----------
       sb_status : str
           New sb_status to be set. Defaults to empty string
       Returns
       -------
       bool
           True if operation was successful, False otherwise
       """
       if len(self.current_task_id) == 0:
           self.logger.error("Worker is not working on a task")
           return False

       temp_task = (
           self.tasks_ref.where("id", "==", self.current_task_id).get()[0].to_dict()
       )
       self.requests_ref.document(temp_task["request_id"]).collection(
           "tasks"
       ).document(temp_task["id"]).update(
           {"sb_status": sb_status}
       )
       return True
   
    
   
    def __update_current_task_status(
        self, status: str, update_all_occurences: bool = False
    ) -> bool:
        """
        Updates status of the current task that the worker is handling

        Parameters
        ----------
        status : str
            New status to be set. Cannot be other that processed_success or processed_failure
        update_all_occurences : bool
            If set to True, all tasks having the same target will be have there status updated.
            Defaults to False

        Returns
        -------
        bool
            True if operationg was successful, False otherwise.

        """
        assert (
            status == "processed_success"
            or status == "download_success"
            or status == "FAILED"
            or status == "processed_failure"
            or status == "in_queue"
            or status == "send_success"
            or status == "pre_success"
        ), f"Status '{status}' is invalid"

        if len(self.current_task_id) == 0:
            self.logger.error("Worker is not working on a task")
            return False

        if update_all_occurences:
            tasks = (
                self.tasks_ref.where("status", "==", "in_queue")
                .where("category", "==", self.current_task["category"])
                .where("target", "==", self.current_task["target"])
                .stream()
            )
            batch = self.db.batch()
            for task in tasks:
                batch.update(
                    task.reference,
                    {
                        "status": "processed_success",
                        "done_by": self.email,
                        "updated": datetime.datetime.utcnow(),
                    },
                )
            batch.commit()

            tasks = (
                self.tasks_ref.where("status", "==", "in_process")
                .where("category", "==", self.current_task["category"])
                .where("target", "==", self.current_task["target"])
                .stream()
            )
            batch = self.db.batch()
            for task in tasks:
                batch.update(
                    task.reference,
                    {
                        "status": "processed_success",
                        "done_by": self.email,
                        "updated": datetime.datetime.utcnow(),
                    },
                )
            batch.commit()

            temp_task = (
                self.tasks_ref.where("id", "==", self.current_task_id).get()[0].to_dict()
            )

            self.requests_ref.document(temp_task["request_id"]).collection(
                "tasks"
            ).document(temp_task["id"]).update(
                {
                    "status": status,
                    "updated": datetime.datetime.utcnow(),
                    "done_by": self.email,
                }
            )

        else:
            temp_task = (
                self.tasks_ref.where("id", "==", self.current_task_id).get()[0].to_dict()
            )

            self.requests_ref.document(temp_task["request_id"]).collection(
                "tasks"
            ).document(temp_task["id"]).update(
                {"status": status, "updated": datetime.datetime.utcnow(), "done_by": ""}
            )

        return True

    def __fetch_entity(self, entity_id: str):
        """
        Method to return document from firestore

        Parameters
        ----------
        entity_id : str
            Document ID of document stored in "entities" collection.

        Returns
        -------
        dict
            Entity document.

        """
        return self.entities_ref.document(entity_id).get().to_dict()

    

    def __fetch_ppl(self, ppl_id: str):
        """
        Method to return document from firestore

        Parameters
        ----------
        ppl_id : str
            Document ID of document stored in "ppl" collection.

        Returns
        -------
        dict
            ppl document.

        """
        return self.ppl_ref.document(ppl_id).get().to_dict()
    
    def __fetch_search_ppl(self, search_ppl_id: str):
        """
        Method to return document from firestore

        Parameters
        ----------
        search_ppl_id : str
            Document ID of document stored in "ppl_search" collection.

        Returns
        -------
        dict
            ppl_search document.

        """
        return self.ppl_search_ref.document(search_ppl_id).get().to_dict()    


    def __fetch_advanced_search_ppl(self, advanced_search_ppl_id: str):
        """
        Method to return document from firestore

        Parameters
        ----------
        advanced_search_ppl_id : str
            Document ID of document stored in "ppl_search_advanced" collection.

        Returns
        -------
        dict
            ppl_search document.

        """
        return self.advanced_ppl_search_ref.document(advanced_search_ppl_id).get().to_dict()    

    def __fetch_profile_search_advanced(self,profile_search_advanced: str):
        """
        Method to return document from firestore

        Parameters
        ----------
        profile_search_advanced : str
            DESCRIPTION.

        Returns
        -------
        list
            profile_search_advanced document.

        """
        return self.advanced_ppl_search_ref.document(profile_search_advanced).get().to_dict()  
    
    def __fetch_advanced_search_companies(self, advanced_search_company_id: str):
        """
        Method to return document from firestore

        Parameters
        ----------
        advanced_search_company_id : str
            Document ID of document stored in "ppl_search_advanced" collection.

        Returns
        -------
        dict
            ppl_search document.

        """
        return self.advanced_company_search_ref.document(advanced_search_company_id).get().to_dict()    

    def __fetch_company_search_advanced(self,company_search_advanced: str):
        """
        Method to return document from firestore

        Parameters
        ----------
        company_search_advanced : str
            DESCRIPTION.

        Returns
        -------
        list
            profile_search_advanced document.

        """
        return self.advanced_company_search_ref.document(company_search_advanced).get().to_dict()
 
    
    def __fetch_25_months_employees(self,insights_hist: str):
            """
            Method to return document from firestore
    
            Parameters
            ----------
            profile_search_advanced : str
                DESCRIPTION.
    
            Returns
            -------
            list
                profile_search_advanced document.
    
            """
            return self.entities_ref.document(insights_hist).get().to_dict()
        
     
    
    def __entity_needs_update(self, entity: dict):
        """
        Checks if entity needs update. Generally, if the collection date is
        that of today or yesterday, no need to update.

        Parameters
        ----------
        entity : dict
            Entity to be checked.

        Returns
        -------
        bool
            True if needs update, False otherwise.

        """
        if not 'about' in entity.keys() :
            return True
        
        if len(entity['about']) ==2:
            return True
        try:
            date_collected = entity["about"]["date_collected"]
        except:
            two_days_ago = (datetime.datetime.now() - datetime.timedelta(2)).strftime("%d-%b-%y")
            date_collected = two_days_ago
        today = date.today().strftime("%d-%b-%y")
        yesterday = (datetime.datetime.now() - datetime.timedelta(1)).strftime(
            "%d-%b-%y"
        )

        return not (date_collected == today or date_collected == yesterday)

    
    def __ppl_needs_update(self, ppl: dict):
        """
        Checks if ppl needs update. Generally, if the collection date is
        that of today or yesterday, no need to update.

        Parameters
        ----------
        ppl : dict
            ppl to be checked.

        Returns
        -------
        bool
            True if needs update, False otherwise.

        """
        if not 'contact_info' in ppl.keys():
            return True
        try:
            date_collected = ppl["ppl"]["date_collected"]
        except:

            return True
        today = date.today().strftime("%d-%b-%y")
        yesterday = (datetime.datetime.now() - datetime.timedelta(1)).strftime(
            "%d-%b-%y"
        )

        return not (date_collected == today or date_collected == yesterday)

    def __check_if_worker_needs_validation(self, about_message: object):
        """
        Checks if worker needs validation based on message returned from getAbouts function.
        If worker needs validation, status is updated in firestore.

        Parameters
        ----------
        about_message : object
            Can be either dictionary or str.

        Returns
        -------
        bool
            Returns True if account needs validation, False otherwise.

        """
        if about_message == "security_check":
            self.__update_worker_status("needs validation")
            return True

        return False

    def __pre_process_about_numerical_data(self, data: dict):
        """
        Prepares new about to be stored in firestore

        Parameters
        ----------
        data : dict
            About data.

        Returns
        -------
        doc : dict
            New entity.

        """
        new_about = data["about"][0]
        new_about["date_collected"] = datetime.datetime.strftime(
            datetime.datetime.today(), "%d-%b-%y"
        )


        if "about" in new_about["updated_Link"].split("/"):
            x = new_about["updated_Link"].split("/")
            x.remove("about")
            new_about["updated_Link"] = "/".join(x)

        doc = self.__fetch_entity(self.current_task['target'])

        # print("THIS IS THE DOC PRIN1TED HHHHH:        ",doc)
        if doc is None:
            doc = {"about": []}
            doc["about"].append(
                {
                    "updated_Link": "",
                    
                    "date_collected": "",
                    
                    "numericLink":""
                    
                }
            )
            
            
        
            
            
            

        # if not "numberOfEmployees_history" in doc.keys():
        #     doc["numberOfEmployees_history"] = dict()

        # doc["numberOfEmployees_history"][new_about["date_collected"]] = int(
        #     new_about["numberOfEmployees"]
        # )

        doc["about"] = new_about
        if 'client' in data.keys():
            doc['client']=data['client']
        
   

        doc["last_updated"] = datetime.datetime.utcnow()
        # doc["id"]= 

        return doc

    def __pre_process_about_data(self, data: dict):
        """
        Prepares new about to be stored in firestore

        Parameters
        ----------
        data : dict
            About data.

        Returns
        -------
        doc : dict
            New entity.

        """
        new_about = data["about"][0]
        new_about["date_collected"] = datetime.datetime.strftime(
            datetime.datetime.today(), "%d-%b-%y"
        )


        if "about" in new_about["updated_Link"].split("/"):
            x = new_about["updated_Link"].split("/")
            x.remove("about")
            new_about["updated_Link"] = "/".join(x)

        doc = self.__fetch_entity(self.current_task['target'])

        if doc is None:
            doc = {"about": []}
            doc["about"].append(
                {
                    "updated_Link": "",
                    "Overview": "",
                    "Website": "",
                    "Industry": "",
                    "Headquarters": "",
                    "CompanySize": "",
                    "CompType": "",
                    "Founded": "",
                    "Speciality": "",
                    # "Phone":"",
                    "location": "",
                    "Warning": "",
                    "date_collected": "",
                    "numberOfEmployees": 0,
                    "company_logo_link": "",
                    "verified":"",
                    
                }
            )
        
            
            
            

        if not "numberOfEmployees_history" in doc.keys():
            doc["numberOfEmployees_history"] = dict()

        doc["numberOfEmployees_history"][new_about["date_collected"]] = int(
            new_about["numberOfEmployees"]
        )

        doc["about"] = new_about
        if 'client' in data.keys():
            doc['client']=data['client']
        
   

        doc["last_updated"] = datetime.datetime.utcnow()

        return doc


    def __pre_process_about_data_for_ppl(self, data: dict, ref_id):
        """
        Prepares new about to be stored in firestore ()

        Parameters
        ----------
        data : dict
            About data.
        
        ref_id: str
            LI id of document where to store the data

        Returns
        -------
        doc : dict
            New entity.

        """
        new_about = data["about"][0]
     
        new_about["date_collected"] = datetime.datetime.strftime(
            datetime.datetime.today(), "%d-%b-%y"
        )

        if "about" in new_about["updated_Link"].split("/"):
            x = new_about["updated_Link"].split("/")
            x.remove("about")
            new_about["updated_Link"] = "/".join(x)

        doc = self.__fetch_entity(ref_id)

        if doc is None:
            doc = {"about": []}
            doc["about"].append(
                {
                    "updated_Link": "",
                    "Overview": "",
                    "Website": "",
                    "Industry": "",
                    "Headquarters": "",
                    "CompanySize": "",
                    "CompType": "",
                    "Founded": "",
                    "Speciality": "",
                    # "Phone":"",
                    "location": "",
                    "Warning": "",
                    "date_collected": "",
                    "numberOfEmployees": 0,
                    "company_logo_link": "",
                    "verified":"",
                }
            )

        if not "numberOfEmployees_history" in doc.keys():
            doc["numberOfEmployees_history"] = dict()

        doc["numberOfEmployees_history"][new_about["date_collected"]] = int(
            new_about["numberOfEmployees"]
        )

        doc["about"] = new_about
               

        doc["last_updated"] = datetime.datetime.utcnow()

        return doc




    def __pre_process_ppl_data(self, data: dict):
        """
        Prepares new ppl to be stored in firestore

        Parameters
        ----------
        data : dict
            ppl data.

        Returns
        -------
        doc : dict
            New ppl.

        """
        
        
     

        #job_type="ppl"
        #if data['general']['name']=='':
        #    return -1
          

        #doc = self.__fetch_ppl(self.current_task['target'])

        #if doc is None:
        
            
            #default = {'contact_info':{"email":"", "websites": "", "twitter":"","phone":""}, "general": {"linkedin_url":"", "name": "", "header":"","location":"", "profile_pic_link":""}, "experience":[], "education":[]}
            #db.collection(job_type).document(self.current_task['target']).set(default)
  
            
        data["founder_jobs"]=[]
        data["ppl"]={}
        
        #create tasks
        if "experience" in data.keys():
            for exp in data['experience']:
                
                company_linkedin_url=exp['company_linkedin_url']
                
                if  exp['title'] is not None and "founder" not in str.lower(exp['title']):
                    continue
                
                founder_dict={'title': exp['title'], 'started_date': exp['period_start'],'linkedin_url':company_linkedin_url}
                data["founder_jobs"].append(founder_dict)
             
            
       
 
        data["ppl"]["date_collected"] = datetime.datetime.strftime(datetime.datetime.today(), "%d-%b-%y")


        return data


    def __store_num_about_in_firestore(self, new_entity: dict) -> None:
        """
        Pushes the document into firestore.

        Parameters
        ----------
        new_entity : dict
            Complete entities document.

        Returns
        -------
        None.

        """
        new_entity["last_updated"]=datetime.datetime.utcnow()
        if 'parallel_number' not in new_entity.keys():
            new_entity['parallel_number']=randint(1, 10)
        
       
        try:
            id_here = new_entity['about']['updated_Link'].split("/company/")[1].rstrip("/")
        except:
            try:
                id_here = new_entity['about']['updated_Link'].split("/school/")[1].rstrip("/")
            except:
                id_here = new_entity['about']['updated_Link'].split("/showcase/")[1].rstrip("/")
            
            
        if 'id' not in new_entity.keys():
            new_entity['id'] = id_here
            
            
        self.entities_ref.document(id_here).set(new_entity, merge=True)
        
        # except:
        #     new_entity["id"] = 

    def __store_sb_in_firestore(self, new_entity: dict, document_id: str) -> None:
        # Initialize default values
        founded_year = None
        main_founding_year_updated = False
        sb_updated = {}
    
        # Check if 'founded' exists in new_entity['about'] and process it
        if 'Founded' in new_entity['about']:
            founded = new_entity['about']['Founded'].strip()
            main_founding_year_updated = bool(founded)
            
            try:
                # Extract the year from the last word in the founded string
                founded_year = int(founded.split()[-1])
                sb_updated = {
                "sb_updated": {
                    "main_founding_year_updated": main_founding_year_updated,
                    
                    "created": datetime.datetime.utcnow()
                }
            }
            except :
                # If the conversion fails, reset the year and update flag
                founded_year = None
                sb_updated = {}
    
        # Prepare the sb_updated dictionary
        if "sb_updated" in sb_updated:
            try:
               self.entities_ref.document(new_entity["id"]).set(sb_updated,merge=True)
               document_id = new_entity["id"]
            except:
                try:
                    document_id = new_entity['about']['updated_Link'].split("/company/")[1].rstrip("/")
                except:
                    try:
                        document_id = new_entity['about']['updated_Link'].split("/school/")[1].rstrip("/")
                    except:
                        document_id = new_entity['about']['updated_Link'].split("/showcase/")[1].rstrip("/")
                        
            self.entities_ref.document(document_id).set(sb_updated,merge=True)    
            
        
        
        
    def __store_sb_in_supabase(self, new_entity: dict, document_id: str) -> None:
        # supabase = connect_to_supabase()
    
        # # Check if connection was successful
        # if supabase == -1 or supabase == -2:
        #     print("Supabase connection failed")
        #     return
        # Check if 'about' exists and is a dictionary
        about_data = new_entity.get('about', None)
        # print("***************about data:    ",about_data)
        # if about_data and isinstance(about_data, dict):  # Expecting a dictionary
        #     print(f"numberOfEmployees is: {about_data.get('numberOfEmployees', 'N/A')}")
        # else:
        #     print("No valid 'about' data found.")
        #     return  # Exit early if 'about' data is invalid
    
        # Extract domain from the 'Website' field
        try:
            website_url = about_data.get('Website', '').strip()
            if website_url is not None or website_url !='':
                domain = None
                if website_url:
                    domain = extract_domain(website_url)
                    if domain == -2:
                        if website_url == "https://" or website_url == "http://":
                            domain = None
                    if domain == -3:
                        domain = None
                    print(f"Extracted domain: {domain}")
            else:
                website_url
                domain = None
        except:
            website_url = None
            domain= None
        
        # Process the 'Founded' data
        founded = about_data.get('Founded', '').strip()  # Keep it as a string
    
        # If 'Founded' is empty or invalid, set main_founding_year_value to None
        if not founded:
            main_founding_year_value = None
        else:
            # Attempt to convert the 'Founded' string to an integer (for the year)
            try:
                main_founding_year_value = int(founded) if founded.isdigit() else None
            except ValueError:
                main_founding_year_value = None  # If it fails, set it to None
    
         
            
            
        # Log the founded year to check for debugging
        print(f"Main founding year: {main_founding_year_value}")
    
        updated_link = about_data.get('updated_Link', '')
        if 'numericLink' in about_data:
            numerical_link = about_data.get("numericLink",'')
            
        
        else:
            numerical_link = ""
        if document_id:
            # Create a Raw_Entity object from the new_entity, including the domain if available
            raw_entity = Raw_Entity(
                Name=about_data.get('Name', ''),
                Founded=main_founding_year_value,  # Pass None if no valid founding year
                date_collected=about_data.get('date_collected', ''),
                Headquarters=about_data.get('Headquarters', ''),
                CompType=about_data.get('CompType', ''),
                Website=domain,
                numericLink=numerical_link,
                Speciality=about_data.get('Speciality', ''),
                location=about_data.get('location', ''),
                Overview=about_data.get('Overview', ''),
                numberOfEmployees=about_data.get('numberOfEmployees', 0),
                verified=about_data.get('verified', ''),
                updated_Link=about_data.get('updated_Link', ''),
                Industry=about_data.get('Industry', ''),
                CompanySize=about_data.get('CompanySize', ''),
               
            )
    
            # Call the update function with the main founding year as None if not available
            try:
                result = update_oldest_founder_founding_if_older_add_about(
                    li_id=document_id,  # Now 'ernstandyoung' without '/about'
                    raw_entity=raw_entity,
                    new_year=None,  # No specific new year since this is for the main founding date
                    new_month=None,  # Month is not available
                    main_founding_year_value=main_founding_year_value,  # Pass None if no founding year
                    create_if_missing=True
                )
                print(f"Supabase update result for {document_id}: {result}")
            except Exception as e:
                print(f"Error updating Supabase for {document_id}: {str(e)}")
        else:
            print("No valid LinkedIn ID found, skipping update.")



            
    def __store_entity_in_firestore(self, new_entity: dict) -> None:
        """
        Pushes the document into firestore.

        Parameters
        ----------
        new_entity : dict
            Complete entities document.

        Returns
        -------
        None.

        """
        new_entity["last_updated"]=datetime.datetime.utcnow()
        if 'parallel_number' not in new_entity.keys():
            new_entity['parallel_number']=randint(1, 10)
                
        try:
            self.entities_ref.document(new_entity["id"]).set(new_entity, merge=True)
        except:
            try:
                id_here = new_entity['about']['updated_Link'].split("/company/")[1].rstrip("/")
            except:
                try:
                    id_here = new_entity['about']['updated_Link'].split("/school/")[1].rstrip("/")
                except:
                    id_here = new_entity['about']['updated_Link'].split("/showcase/")[1].rstrip("/")
            
            self.entities_ref.document(id_here).set(new_entity, merge=True)
       
        


    
    def __store_ppl_in_firestore(self, new_ppl: dict) -> None:
        """
        Pushes the document into firestore.

        Parameters
        ----------
        new_ppl : dict
            Complete ppl document.

        Returns
        -------
        None.

        """
        new_ppl["last_updated"]=datetime.datetime.utcnow()
        if 'parallel_number' not in new_ppl.keys():
            new_ppl['parallel_number']=randint(1, 10)
        
        self.ppl_ref.document(new_ppl["id"]).set(new_ppl, merge=True)

   
    def __check_html(self, task: dict) -> bool:
        """
        Check if crawler still working or the HTML changed.

        Parameters
        ----------
        task : dict
            DESCRIPTION.

        Returns
        -------
        bool
            True if operation was successful, False otherwise.

        """

        if not self.logged_in:
            self.logger.warning(
            "Worker is not logged in."
            )
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()


        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )

        
        #for ref=about
        if task['ref']== 'about':
            #entity = self.__fetch_entity(task["target"])
    
            li_id = "https://www.linkedin.com/company/" + task["target"]
    
            new_about = getAbouts(self.driver, li_id)
            if type(new_about) is  dict and "about" in new_about.keys():
                test_about = new_about['about']
                new_about_dict = test_about[0]
                print(new_about_dict)
                if type(new_about_dict) is dict:
                    
                    for one_key  in new_about_dict.keys():
                        if one_key=="numberOfEmployees" or one_key == "date_collected" or one_key=="error" or one_key =="updated_Link":
                            continue
                        if len(new_about_dict[one_key])>0:
                            break
                  
                    else:
                       self.__update_current_task_status("processed_failure", False)
                       return False 
                  
                if not self.__check_if_worker_needs_validation(new_about_dict):
                    
                
                   
                    self.logger.info(
                    f"Done processing task of ID: {task['id']} and of request ID: {task['request_id']}")
                    self.__update_current_task_status("send_success", False)
                    return True
                else:
                    self.logger.critical("Worker needs validation")
                    
                    return "NEEDS_VALIDATION"
       
        elif task['ref']== 'ppl':
            li_id = "https://www.linkedin.com/in/" + task["target"]
            ##
            new_ppl = getppl(self.driver, li_id) 
            # print("new_ppl:  ",new_ppl)
            
            if type(new_ppl) is  dict and "general" in new_ppl.keys():
                for one_key  in new_ppl["general"].keys():
                    if one_key=="linkedin_url" or  one_key=="numberOfConnections" :
                        continue                            
                        
                    if len(new_ppl["general"][one_key])>0:
                        break
              
                else:
                    #self.__update_current_task_status("processed_failure", False)
                    #return False 
                    
                
                    if "contact_info" in new_ppl.keys():  
                        for one_key  in new_ppl["contact_info"].keys():
                            if one_key=="error" :
                                continue
                            if len(new_ppl["contact_info"][one_key])>0:
                                break
                      
                        else:
                    
                            if "experience" in new_ppl.keys() and len(new_ppl["experience"])>0:
                                if not self.__check_if_worker_needs_validation(new_ppl):
                                    self.logger.info(
                                    f"Done processing task of ID: {task['id']} and of request ID: {task['request_id']}")
                                    self.__update_current_task_status("send_success", False)
                                    return True
                                else:
                                    self.logger.critical("Worker needs validation")
                                    
                                    return "NEEDS_VALIDATION"               
                            if "education" in new_ppl.keys() and len(new_ppl["education"])>0:
                                if not self.__check_if_worker_needs_validation(new_ppl):
                                    self.logger.info(
                                    f"Done processing task of ID: {task['id']} and of request ID: {task['request_id']}")
                                    self.__update_current_task_status("send_success", False)
                                    return True
                                else:
                                    self.logger.critical("Worker needs validation")
                                    
                                    return "NEEDS_VALIDATION"      
                            self.__update_current_task_status("processed_failure", False)
                            return False 
                            
                                                                  
              
            if not self.__check_if_worker_needs_validation(new_ppl):
                self.logger.info(
                f"Done processing task of ID: {task['id']} and of request ID: {task['request_id']}")
                self.__update_current_task_status("send_success", False)
                return True
            else:
                self.logger.critical("Worker needs validation")
                
                return "NEEDS_VALIDATION"
            
    
    
            
        print("!@!@!@! shouldn't get here. but we are, for check_html task (returning False): ",task["id"])
        self.__update_current_task_status("processed_failure", False)
        return False
    
    def __process_ppl(self, task: dict) -> bool:
        """
        Processes a task with category "ppl".

        Parameters
        ----------
        task : dict
            DESCRIPTION.

        Returns
        -------
        bool
            True if operation was successful, False otherwise.

        """

        if not self.logged_in:
            self.logger.warning(
            "Worker is not logged in."
            )
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()


        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )

        ppl = self.__fetch_ppl(task["target"])
        # print("")
        if ppl is not None:
            processed_failure_flag = False
            if not self.__ppl_needs_update(ppl):
                if ppl["experience"]:
                    for exp in ppl["experience"]:
                        url = exp.get("company_linkedin_url", "")
                        if url and re.match(r".*/company/\d+$", url):
                            processed_failure_flag = True
                            break
                    
                    if not processed_failure_flag:
                        self.logger.warning("No need to re-collect. Skipping")
                        self.__update_current_task_status("processed_success", True)
                        return True

        li_id = "https://www.linkedin.com/in/" + task["target"]
        print("li_id is: ",li_id)
        ##
             
        new_ppl = getppl(self.driver, li_id)
        # print("new_pll",new_ppl)
        # print("new_ppl", new_ppl)
        #if type(new_ppl) == str and new_ppl == 'security_check':
            #print("!@!@ wrong format for 'ppl' from getppl. new_ppl=",new_ppl)
        #    return False

        request_type=task['request_id'].split("_")[2]
        
        if request_type=='subscription' and type(new_ppl) == dict:
            
            if task['ref'] == "DIRECT":
                ppl["client"] = {}
            
            else:
                task_client_id=task['client']
                try:
                    timedelta_x=ppl["client"][task_client_id]["about_update_frequency_in_days"]
                    next_about_update_date=datetime.datetime.strftime(datetime.datetime.utcnow() + datetime.timedelta(days=int(timedelta_x)), "%d-%m-%Y") 
                    client=ppl["client"]
                    client[task_client_id]["next_about_update_date"]=next_about_update_date
                    new_ppl.update({"client": client})
    
                except:
                    print("unsuscribe")
                    self.__update_current_task_status("FAILED", False)
                    return True 
                
                
                
        # if new_ppl ==  "page_doesnt_exist":
        #     new_ppl["id"] = task['target']
        #     new_ppl = {}
        #     new_ppl = {"contact_info":{"email":"", "websites": "", "twitter":"","phone":"","error":"page_doesnt_exist"}, "general": {"linkedin_url":li_id, "name": "", "header":"","location":"", "profile_pic_link":"",'pronouns':'','numberOfConnections':0}, "experience":[], "education":[], "ppl":{"date_collected":datetime.datetime.strftime(datetime.datetime.today(), "%d-%b-%y")}}
        #     new_ppl["ppl"]["date_collected"] = datetime.datetime.strftime(datetime.datetime.today(), "%d-%b-%y")
        #     new_ppl["founder_jobs"] = []
        #     new_ppl["ppl"] = {"date_collected": datetime.datetime.strftime(datetime.datetime.today(), "%d-%b-%y")}
        #     self.__store_ppl_in_firestore(new_ppl)
        #     self.__update_current_task_status("processed_success", True)
        #     update_person(li_id, new_ppl)
        if not self.__check_if_worker_needs_validation(new_ppl):
            if new_ppl == "page_doesnt_exist":
                  # self.logger.warning("404: page doesnt exist")
                  #to store the error in firestore
                  default = {}
                  default = {"contact_info":{"email":"", "websites": "", "twitter":"","phone":"","error":"page_doesnt_exist"}, "general": {"linkedin_url":li_id, "name": "", "header":"","location":"", "profile_pic_link":"",'pronouns':'','numberOfConnections':0}, "experience":[], "education":[], "ppl":{"date_collected":datetime.datetime.strftime(datetime.datetime.today(), "%d-%b-%y")}}
                  default["ppl"]["date_collected"] = datetime.datetime.strftime(datetime.datetime.today(), "%d-%b-%y")
                  default["id"]= task["target"]
                  try:
                      self.__store_ppl_in_firestore(default)
                      
                      if task['ref'] == "DIRECT":
                          self.__update_current_task_status("send_success", True)
                      else:
                          self.__update_current_task_status("processed_success", True)
    
                      self.logger.info(f"Done processing task of ID: {task['id']} and of request ID: {task['request_id']}")
                  except Exception as e:
                      print("##*******")
                      print(e)
                      self.__update_current_task_status("processed_failure", False)    
                  
                  return True
            try:
                new_ppl = self.__pre_process_ppl_data(new_ppl)
                new_ppl["id"]= task["target"]
                if not new_ppl.get('experience', []):  # This handles both empty list and missing key
                    self.__update_current_task_status("processed_failure", True)
                    return True

            # 2. Empty titles in experience
                if any(exp.get('title', '').strip() == '' for exp in new_ppl.get('experience', [])):
                    self.__update_current_task_status("processed_failure", True)
                    return True

                else:
                    self.__store_ppl_in_firestore(new_ppl)
                    self.__update_current_task_status("processed_success", True)
                    update_person(task["target"], new_ppl)

            except Exception as e:
                print("##!!!!!!")
                print(e)
                self.__update_current_task_status("processed_failure", False)
                   
            if new_ppl != "page_doesnt_exist" :
                # if new_ppl != "page_doesnt_exist" and new_ppl != "worked":
                # processed_dict = {"processed":{"num_of_profiles":0, "result_main": {"error": "The profile didnt open","error_field":"search_link","result": result  },"detailed_data":True}}
                # processed_dict["last_updated"] = datetime.datetime.utcnow()
                # # self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                if len(new_ppl['experience'])==0:
                    #process failure
                    self.__update_current_task_status("processed_failure", True)
                    return True 
                if 'experience' in new_ppl.keys():
                    for title_check in new_ppl['experience']:
                        # print(title_check)
                        # break
                        if 'title' in title_check:
                            if title_check['title'] == " ":
                                self.__update_current_task_status("processed_failure", True)
                                return True 
                        
                
                self.__update_current_task_status("processed_success", True)
                return True


            self.logger.info(
            f"Done processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )
            # if new_ppl == "worked":
           
            #     self.__update_current_task_status("download_success", True)
            return True
                
            
                
 
            
             
            
              

        #     #check all Education for available websites or collect it otherwise
        #     # else:
        #     #     if "education" in new_ppl.keys():
        #     #         for institution in new_ppl["education"]:
        #     #             if "linkedin.com" not in institution["institution_linkedin_url"]:
        #     #                 continue
                        
        #     #             if institution["institution_url"] in ["","-"]:
                          
        #     #                 try:
        #     #                     li_id = institution["institution_linkedin_url"].split("/company/")[1].split("/")[0]
        #     #                 except:
        #     #                     li_id = institution["institution_linkedin_url"].split("/school/")[1].split("/")[0]    
        #     #                 #print("## li_id:",li_id)
        #     #                 li_link = "https://www.linkedin.com/company/" + li_id
        #     #                 new_about = getAbouts(self.driver, li_link)
                            
                            
        #     #                 if not self.__check_if_worker_needs_validation(new_about):
        #     #                     try:
        #     #                         new_entity = self.__pre_process_about_data_for_ppl(new_about, li_id)
        #     #                         self.__store_entity_in_firestore(new_entity)
        #     #                         #store the url inside new_ppl
        #     #                         #print("## after __store_entity_in_firestore")
        #     #                         institution["institution_url"]=new_entity["about"]["Website"]
        #     #                     except Exception as e:
        #     #                         print("##$$$$!")
        #     #                         print(e)                                    
                                                        
        #     #                 else:
        #     #                     self.logger.critical("Worker needs validation")
        #     #                     return "NEEDS_VALIDATION"
                
                            
                    
        
        #     try:
        #         new_ppl = self.__pre_process_ppl_data(new_ppl)
        #         new_ppl["id"]= task["target"]
        #         self.__store_ppl_in_firestore(new_ppl)
    
        #         self.__update_current_task_status("processed_success", True)
        #     except Exception as e:
        #         print("##!!!!!!")
        #         print(e)
        #         self.__update_current_task_status("processed_failure", False)

        #     self.logger.info(
        #     f"Done processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        # )
        else:
            self.logger.critical("Worker needs validation")
            return "NEEDS_VALIDATION"

    def __process_search_ppl(self, task: dict) -> bool:
        """
        Processes a task with category "search_people".

        Parameters
        ----------
        task : dict
            DESCRIPTION.

        Returns
        -------
        bool
            True if operation was successful, False otherwise.

        """

        if not self.logged_in:
            self.logger.warning(
            "Worker is not logged in."
            )
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()


        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )
    

      
        #extract the details of the search from firestore "ppl_search" collection
        search_ref = self.__fetch_search_ppl(task["target"])
        print("search_ref:",search_ref)
        
        
#TODO: check if task new or broken, by checking for stored result
        #if broken, continue={"tab_num":3}
        
        #start building the search string
        #sample: https://www.linkedin.com/search/results/people/?currentCompany=%5B%2210412299%22%5D&origin=FACETED_SEARCH&title=partner
        
        search_string="https://www.linkedin.com/search/results/people/?"
        
        current_company_LI_id_numerical=None
        if search_ref["current_company_LI_id"] != None:
            print('search_ref["current_company_LI_id"] != None')
            temp=get_numericalID(self.driver,self.db,search_ref["current_company_LI_id"])
            
            print('temp:',temp)
            if temp == "Page_doesnt_exist" or temp ==0:
              another_proof = getLink(self.driver,search_ref["current_company_LI_id"])
              if another_proof == "unavailable":

                processed_dict = {"processed":
                                  {"num_of_profiles":1, "result_main": 
                                   {"current_company_LI_id": search_ref["current_company_LI_id"],"past_company_LI_id":search_ref["past_company_LI_id"],"school_LI_id": search_ref["school_LI_id"],
                                                                                    "error":"search_doesnt_exist", "error_field":"current_company_LI_id"},"result": []  }}
                processed_dict["last_updated"] = datetime.datetime.utcnow()
                self.ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                self.__update_current_task_status("processed_success", True)
                return True
            
              if another_proof == "no problem":
                  self.__update_current_task_status("processed_failure", True)
                  return True
              
            if self.__check_if_worker_needs_validation(temp):
                self.logger.critical("Worker needs validation")
                return "NEEDS_VALIDATION"
            
            current_company_LI_id_numerical=temp
            search_string+="currentCompany=%5B%22"+current_company_LI_id_numerical+"%22%5D&"
        
        print(" search string 1:", search_string)
        #geoUrn=%5B"102257491"%2C"102299470"%5D

        if search_ref["location"] != None:
            search_string+="geoUrn=%5B%22"
            for loc in search_ref["location"]:
                 if loc!=search_ref["location"][0]:
                     search_string+="%2C%22"+loc+"%22"
                 else:
                     search_string+=loc+"%22"
            search_string+="%5D&"
        if search_ref["search_industries"] != None:
            search_string+="industry=%5B%22"
            for ind in search_ref["search_industries"]:
                 if ind!=search_ref["search_industries"][0]:
                     search_string+="%2C%22"+ind+"%22"
                 else:
                     search_string+=ind+"%22"
            search_string+="%5D&"
              
        search_string+="origin=FACETED_SEARCH"
        if search_ref["past_company_LI_id"] != None:
            
            temp=get_numericalID(self.driver,self.db,search_ref["past_company_LI_id"])
            if temp == "Page_doesnt_exist" or temp ==0 :
              another_proof = getLink(self.driver,search_ref["current_company_LI_id"])
              if another_proof == "unavailable":


                processed_dict = {"processed":{"num_of_profiles":1, "result_main": {"current_company_LI_id": search_ref["current_company_LI_id"],"past_company_LI_id":search_ref["past_company_LI_id"],"school_LI_id": search_ref["school_LI_id"],"error":"search_doesnt_exist", "error_field":"past_company_LI_id"},"result": []  }}
                processed_dict["last_updated"] = datetime.datetime.utcnow()
                self.ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                self.__update_current_task_status("processed_success", True)
                return True
            
              if another_proof == "no problem":
                  self.__update_current_task_status("processed_failure", True)
                  return True
                  
            if self.__check_if_worker_needs_validation(temp):
                self.logger.critical("Worker needs validation")
                return "NEEDS_VALIDATION"
            
            past_company_LI_id_numerical=temp
             #add it to search string
            search_string+="&pastCompany=%5B%22"+past_company_LI_id_numerical+"%22%5D"
        if search_ref["school_LI_id"] != None:
            temp=get_numericalID(self.driver,self.db,search_ref["school_LI_id"])
            if temp == "Page_doesnt_exist" or temp == 0:
                another_proof = getLink(self.driver,search_ref["current_company_LI_id"])
                if another_proof == "unavailable":
            
                    processed_dict = {"processed":{"num_of_profiles":1, "result_main": {"current_company_LI_id": search_ref["current_company_LI_id"],"past_company_LI_id":search_ref["past_company_LI_id"],"school_LI_id": search_ref["school_LI_id"],"error":"search_doesnt_exist", "error_field":"school_LI_id"},"result": []  }}
                    processed_dict["last_updated"] = datetime.datetime.utcnow()
                    self.ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                    self.__update_current_task_status("processed_success", True)
                    return True
                
                if another_proof == "no problem":
                  self.__update_current_task_status("processed_failure", True)
                  return True
            if self.__check_if_worker_needs_validation(temp):
                self.logger.critical("Worker needs validation")
                return "NEEDS_VALIDATION"
            
            school_LI_id_numerical=temp
            search_string+="&schoolFilter=%5B%22"+school_LI_id_numerical+"%22%5D"
             
        print("!!@@## search_string:", search_string)
        print("pre search_position")
        try:
            result, count, error_message = search_position(self.driver,search_string,search_ref["current_position"],search_ref["max_tabs"], search_ref["strict_current_position"],search_ref["current_company_LI_id"], search_ref["strict_past_company"], search_ref["past_company_LI_id"], current_company_LI_id_numerical )
        except Exception as e:
            print("**********")
            print(e)     
            raise
        print("result:", result)
        print("count:", count)
        print("error_message:",error_message)
        if result == "no_results_container":
            print("⚠️ No results container found - returning task to queue")
            self.__update_current_task_status("in_queue", False)
            return True
        
        if result == "filter_failed":
            print("⚠️ Failed to apply filter - returning task to queue")
            self.__update_current_task_status("in_queue", False)
            return True
        if error_message == "":
            if search_ref["strict_current_position"] or search_ref["strict_past_company"]:
                processed_dict = {"processed":{"num_of_profiles":count, "result_main": {"current_company_LI_id": search_ref["current_company_LI_id"],"past_company_LI_id":search_ref["past_company_LI_id"],"school_LI_id": search_ref["school_LI_id"],"error": None,"error_field":None}}}
                processed_dict["last_updated"] = datetime.datetime.utcnow()
                self.ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)

            else:
                processed_dict = {"processed":{"num_of_pages":count, "result_main": {"current_company_LI_id": search_ref["current_company_LI_id"],"past_company_LI_id":search_ref["past_company_LI_id"],"school_LI_id": search_ref["school_LI_id"],"error": None,"error_field":None}}}
                processed_dict["last_updated"] = datetime.datetime.utcnow()
                self.ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                
            self.__update_current_task_status("download_success", True)
            return True
            
            
        else:
            if self.__check_if_worker_needs_validation(error_message):
                self.logger.critical("Worker needs validation")
#TODO: save the result?
                return "NEEDS_VALIDATION"
            self.__update_current_task_status("processed_failure", False)
            return False
   

    def __process_advanced_search_ppl(self, task: dict) -> bool:
        """
        Processes a task with category "advanced_search_people".
        """
    
        if not self.logged_in:
            self.logger.warning("Worker is not logged in.")
            self.logger.info('Removing all remaining tasks in queue and setting status to "in_queue"')
            self.__remove_all_remaining_tasks_in_queue()
    
        self.logger.debug(f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}")
        
        #extract the details of the search from firestore "ppl_search_advanced" collection
        search_ref = self.__fetch_advanced_search_ppl(task["target"])
        print("search_ref:", search_ref)
    
        if "search_string" in search_ref.keys():
            search_string = search_ref["search_string"]
            search_ref['detailed_data'] = False
        else:
            search_string = "https://www.linkedin.com/sales/search/people?query=(recentSearchParam%3A(id%3A4135502130%2CdoLogHistory%3Atrue)%2Cfilters%3AList("
            
            list_started = False
            is_past_company_search = len(search_ref["past_company_LI_id_included"]) != 0
    
            # 1. POSITION/TITLE SECTION - Always first
            has_positions = len(search_ref["positions_included"]) != 0 or len(search_ref["positions_excluded"]) != 0
            has_free_titles = "free_titles_list" in search_ref and search_ref["free_titles_list"] and search_ref["free_titles_list"] != ""
            
            if has_positions or has_free_titles:
                if list_started:
                    search_string += "%2C"
                search_string += "(type%3ACURRENT_TITLE%2Cvalues%3AList("
                list_started = True
                
                position_count = 0
                # Add positions that have IDs
                if len(search_ref["positions_included"]) != 0:
                    for pos in search_ref["positions_included"]:
                        position_text = pos.replace(" ", "%2520")
                        if position_count > 0:
                            search_string += "%2C"
                        search_string += "(id%3A" + str(search_ref["positions_included"][pos]) + "%2Ctext%3A" + position_text + "%2CselectionType%3AINCLUDED)"
                        position_count += 1
                
                # Add free titles
                if has_free_titles:
                    free_titles = [title.strip() for title in search_ref["free_titles_list"].split(",")]
                    for title in free_titles:
                        if position_count > 0:
                            search_string += "%2C"
                        
                        # Encode the title properly
                        encoded_title = title
                        if ' ' in encoded_title:
                            if '&' in encoded_title:
                                encoded_title = encoded_title.replace("&", "%2526")
                            encoded_title = encoded_title.replace(" ", "%2520")
                        
                        search_string += "(text%3A" + encoded_title + "%2CselectionType%3AINCLUDED)"
                        position_count += 1
                
                search_string += ")"
        
                if search_ref["subfilter_position"] == "current":
                    search_string += "%2CselectedSubFilter%3ACURRENT)"
                elif search_ref["subfilter_position"] == "past":
                    search_string += "%2CselectedSubFilter%3APAST)"
                elif search_ref["subfilter_position"] == "current or past":
                    search_string += "%2CselectedSubFilter%3ACURRENT_OR_PAST)"    
                else:
                    search_string += "%2CselectedSubFilter%3APAST_NOT_CURRENT)"
    
            # 2. HEADQUARTERS SECTION
            if search_ref["company_headquarters_location"] is not None and search_ref["company_headquarters_location"]["text"] != '*' and search_ref["company_headquarters_location"]["geoUrn"] != '*':
                if list_started:
                    search_string += "%2C"
                search_string += "(type%3ACOMPANY_HEADQUARTERS%2Cvalues%3AList("
                list_started = True
    
                try:
                    location_id = search_ref["company_headquarters_location"]["geoUrn"]
                    location_text = search_ref["company_headquarters_location"]["text"]
                    if ' ' in location_text:
                        if '&' in location_text:
                            location_text = location_text.replace("&", "%2526")
                        location_text = location_text.replace(" ", "%2520")
                        
                    search_string += "(id%3A" + str(location_id) + "%2Ctext%3A" + location_text + "%2CselectionType%3AINCLUDED)"
                    search_string += "))"
                except Exception as e:
                    processed_dict = {"processed":
                                      {"num_of_profiles": 1, "result_main": {
                                        "error": "search_doesnt_exist", 
                                        "error_field": "company_headquarters_location"}
                                          , "result": [], "search_link": search_string}, "detailed_data": search_ref["detailed_data"]}
                    processed_dict["last_updated"] = datetime.datetime.utcnow()
                    self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                    self.__update_current_task_status("processed_success", True)
                    return True
    
            # 3. SCHOOL SECTION    
            if search_ref["school"] is not None and search_ref["school"]["text"] != '*' and search_ref["school"]["numericID"] != '*':
                if list_started:
                    search_string += "%2C"
                search_string += "(type%3ASCHOOL%2Cvalues%3AList("
                list_started = True
    
                try:
                    school_id = search_ref["school"]["numericID"]
                    school_text = search_ref["school"]["text"]
                    if ' ' in school_text:
                        if '&' in school_text:
                            school_text = school_text.replace("&", "%2526")
                        school_text = school_text.replace(" ", "%2520")
                        
                    search_string += "(id%3A" + str(school_id) + "%2Ctext%3A" + school_text + "%2CselectionType%3AINCLUDED)"
                    search_string += "))"
                except Exception as e:
                    processed_dict = {"processed":
                                      {"num_of_profiles": 1, "result_main": {
                                        "error": "search_doesnt_exist", 
                                        "error_field": "school"}
                                          , "result": [], "search_link": search_string}, "detailed_data": search_ref["detailed_data"]}
                    processed_dict["last_updated"] = datetime.datetime.utcnow()
                    self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                    self.__update_current_task_status("processed_success", True)
                    return True
    
            # 4. CURRENT COMPANY SECTION
            if len(search_ref["current_company_LI_id_included"]) != 0 or len(search_ref["current_company_LI_id_excluded"]) != 0:
                if list_started:
                    search_string += "%2C"
                search_string += "(type%3ACURRENT_COMPANY%2Cvalues%3AList("
                list_started = True
                company_count = 0
                
                if len(search_ref["current_company_LI_id_included"]) != 0:
                    for pos in search_ref["current_company_LI_id_included"]:
                        temp, name_temp = get_name_and_numericalID(self.driver, self.db, pos)
                        if ' ' in name_temp:
                            if '&' in name_temp:
                                name_temp = name_temp.replace("&", "%2526")  # Double encode the &
                            name_temp = name_temp.replace(" ", "%2520")
                        
                        if temp == "Page_doesnt_exist" or temp == 0:
                            processed_dict = {"processed":
                                              {"num_of_profiles": 1, "result_main": {
                                                "error": "search_doesnt_exist", 
                                                "error_field": "current_company_LI_id_included"}
                                                  , "result": [], "search_link": search_string}, "detailed_data": search_ref["detailed_data"]}
                            processed_dict["last_updated"] = datetime.datetime.utcnow()
                            self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                            self.__update_current_task_status("processed_success", True)
                            return True
                        if self.__check_if_worker_needs_validation(temp):
                            self.logger.critical("Worker needs validation")
                            return "NEEDS_VALIDATION"                    
    
                        current_company_LI_id_numerical = temp
                        current_company_name = name_temp
                        if company_count > 0:
                            search_string += "%2C"
                        search_string += "(id%3Aurn%253Ali%253Aorganization%253A" + current_company_LI_id_numerical + "%2Ctext%3A" + current_company_name + "%2CselectionType%3AINCLUDED%2Cparent%3A(id%3A0))"
                        company_count += 1
    
                search_string += "))"
    
            # 5. YEARS SECTION
            if len(search_ref["years_in_current_company"]) > 0 and search_ref["years_in_current_company"] != "All":
                if list_started:
                    search_string += "%2C"
                search_string += "(type%3AYEARS_AT_CURRENT_COMPANY%2Cvalues%3AList("
                list_started = True           
                if search_ref["years_in_current_company"] == "Less than 1 year":
                    search_string += "(id%3A1%2Ctext%3ALess%2520than%25201%2520year%2CselectionType%3AINCLUDED)"    
                elif search_ref["years_in_current_company"] == "1 to 2 years":
                    search_string += "(id%3A2%2Ctext%3A1%2520to%25202%2520years%2CselectionType%3AINCLUDED)"  
                elif search_ref["years_in_current_company"] == "3 to 5 years":
                    search_string += "(id%3A3%2Ctext%3A3%2520to%25205%2520years%2CselectionType%3AINCLUDED)"
                elif search_ref["years_in_current_company"] == "6 to 10 years":
                    search_string += "(id%3A4%2Ctext%3A6%2520to%252010%2520years%2CselectionType%3AINCLUDED)"
                else:
                    search_string += "(id%3A5%2Ctext%3AMore%2520than%252010%2520years%2CselectionType%3AINCLUDED)"
                search_string += "))"
    
            # 6. PAST COMPANY SECTION - Stays last
            if is_past_company_search:
                if list_started:
                    search_string += "%2C"
                search_string += "(type%3APAST_COMPANY%2Cvalues%3AList("
                list_started = True
                company_count = 0
    
                if len(search_ref["past_company_LI_id_included"]) != 0:
                    for pos in search_ref["past_company_LI_id_included"]:
                        temp, name_temp = get_name_and_numericalID(self.driver, self.db, pos)
                        if ' ' in name_temp:
                            if '&' in name_temp:
                                name_temp = name_temp.replace("&", "%2526")
                            name_temp = name_temp.replace(" ", "%2520")
                        if temp == "Page_doesnt_exist" or temp == 0:
                            processed_dict = {"processed":
                                              {"num_of_profiles": 1, "result_main": {
                                                "error": "search_doesnt_exist", "error_field": "past_company_LI_id_included"}, "result": [], "search_link": search_string}, "detailed_data": search_ref["detailed_data"]}
                            processed_dict["last_updated"] = datetime.datetime.utcnow()
                            self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                            self.__update_current_task_status("processed_success", True)
                            return True
                        if self.__check_if_worker_needs_validation(temp):
                            self.logger.critical("Worker needs validation")
                            return "NEEDS_VALIDATION"                    
    
                        past_company_LI_id_numerical = temp
                        past_company_name = name_temp
                        
                        if company_count > 0:
                            search_string += "%2C"
                        search_string += "(id%3Aurn%253Ali%253Aorganization%253A" + past_company_LI_id_numerical + "%2Ctext%3A" + past_company_name + "%2CselectionType%3AINCLUDED)"
                        company_count += 1
    
                search_string += "))"
    
            # Close the filters list
            search_string += "))"
    
        print("!!@@## search_string:", search_string)
        print("pre advanced_search_position")
        try:
            result, count, error_message = sales_search(self.driver,search_string,search_ref["detailed_data"],search_ref["max_tabs"],search_ref["return_counts_only"])
        except Exception as e:
            print("**********")
            print(e)     
            raise
        
        if result == "security_check":
           
            self.__update_worker_status("needs validation")
            return True
        
        
        # if len(result) == 25 and search_ref['max_tab']>=2:
        #     self.__update_current_task_status("processed_failure", True)
        #     return False
        if search_ref['return_counts_only']==False:
            if error_message == "":
                if search_ref["detailed_data"]:
                    processed_dict = {"processed":{"num_of_profiles":count, "result_main": {"error": None,"error_field":None,"result": result,"search_link":search_string  },"detailed_data":search_ref["detailed_data"]}}
                    processed_dict["last_updated"] = datetime.datetime.utcnow()
                    self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
    
                else:
                    processed_dict = {"processed":{"num_of_pages":count, "result_main": {"error": None,"error_field":None,"result": result,"search_link":search_string  },"detailed_data":search_ref["detailed_data"]}}
                    processed_dict["last_updated"] = datetime.datetime.utcnow()
                    self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                self.__update_current_task_status("processed_success", True)
                return True
                
                
            else:
                if self.__check_if_worker_needs_validation(error_message):
                    self.logger.critical("Worker needs validation")
    #TODO: save the result?
                    return "NEEDS_VALIDATION"
                self.__update_current_task_status("processed_failure", False)
                # if result == []:
                # processed_dict = {"processed":{"num_of_profiles":0, "result_main": {"error": None,"error_field":"search_link does not exist","result": [],"search_link":search_string  },"detailed_data":search_ref["detailed_data"]}}
                # processed_dict["last_updated"] = datetime.datetime.utcnow()
                # self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
        
                return False
            
            
            
        elif search_ref['return_counts_only']==True:
            if error_message == "":
                
                processed_dict = {"processed":{"num_of_pages":count, "result_main": {"error": None,"error_field":None,"result": [],"search_link":search_string},"search_total_results":result['search_total_results'],"detailed_data":search_ref["detailed_data"]}}
                processed_dict["last_updated"] = datetime.datetime.utcnow()
                self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                self.__update_current_task_status("processed_success", True)
                return True
                
                
            else:
                if self.__check_if_worker_needs_validation(error_message):
                    self.logger.critical("Worker needs validation")
    #TODO: save the result?
                    return "NEEDS_VALIDATION"
                self.__update_current_task_status("processed_failure", False)
                return False
            
            
        else:
            raise
            
    def __process_new_profile_search_ppl_advanced(self, task: dict) -> bool:
        """
        Process new profile search with additional database checking functionality.
        
        Parameters
        ----------
        task : dict
            Task information containing search parameters.
        Returns
        -------
        bool
            True if operation was successful, False otherwise
        """
        global counter
        if 'counter' not in globals():
            counter = 0
        if not self.logged_in:
            self.logger.warning("Worker is not logged in.")
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()
            counter = 0 
        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )
    
        # Extract the details of the search from firestore "ppl_search_advanced" collection
        search_ref = self.__fetch_profile_search_advanced(task["target"])
        print("search_ref:", search_ref)
        print("salelink: ", search_ref['profile_link'])
        
        # Check for existing profiles with the same link
        existing_profiles = (
            self.db.collection("ppl_search_advanced")
            .where("profile_link", "==", search_ref['profile_link'])
            .get()
        )
        
        # Check if we have existing profiles that were processed recently
        current_time = datetime.datetime.utcnow()
        for profile in existing_profiles:
            profile_data = profile.to_dict()
            
            # Skip if it's the current document
            if profile.id == search_ref['id']:
                continue
                
            # Check if the profile has processed results
            if ('processed' in profile_data and 
                'result_main' in profile_data['processed'] and 
                'result' in profile_data['processed']['result_main'] and
                'last_updated' in profile_data):
                
                # Convert timezone-aware datetime to UTC for comparison
                last_updated = profile_data['last_updated'].replace(tzinfo=None)
                time_difference = current_time - last_updated
                if time_difference.total_seconds() < (31 * 24 * 60 * 60):  # 60 hours in seconds
                    # Copy the results to our current document
                    update_data = {"processed":
                                   {"num_of_profiles":1, "result_main": 
                                    {"error": None,"error_field":None,
                                     "result": profile_data['processed']['result_main']['result'] }
                                    ,"detailed_data":True},
                        "last_updated": current_time
                    }
                    
                    # Update the current document
                    self.db.collection("ppl_search_advanced").document(search_ref['id']).set(
                        update_data, merge=True
                    )
                    
                    # Update task status and return
                    self.__update_current_task_status("processed_success", True)
                    counter += 1
                    print(f"Task completed successfully (reused data). Total tasks processed: {counter}")
                    return True
        
        
        
        try:
            result= advanced_search_people_profile_html(self.driver,search_ref['profile_link'],search_ref['id'])
        except Exception as e:
            print("**********")
            print(e)     
            raise
        print("result:", result)

        if result == "worked":
           
            self.__update_current_task_status("download_success", True)
            counter += 1
            print(f"Task completed successfully. Total tasks processed: {counter}")
            return True
            
        else:
            # processed_dict = {"processed":{"num_of_profiles":0, "result_main": {"error": "The profile didnt open","error_field":"search_link","result": result  },"detailed_data":True}}
            # processed_dict["last_updated"] = datetime.datetime.utcnow()
            # # self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)

            
            self.__update_current_task_status("processed_failure", True)
            counter += 1
            print(f"Task completed failed. Total tasks processed: {counter}")
            return True
            
            
#         else:
#             if self.__check_if_worker_needs_validation(result):
#                 self.logger.critical("Worker needs validation")
# #TODO: save the result?
#                 return "NEEDS_VALIDATION"
#             self.__update_current_task_status("processed_failure", False)
#             return False
        
     
    def __process_profile_search_ppl_advanced(self,task:dict) -> bool:
        """
        

        Parameters
        ----------
        task : dict
            DESCRIPTION.

        Returns
        -------
        bool
            True if operation was successful,False otherwise
            

        """
        if not self.logged_in:
            self.logger.warning(
            "Worker is not logged in."
            )
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()


        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )
    

      
        #extract the details of the search from firestore "ppl_search_advanced" collection
        search_ref = self.__fetch_profile_search_advanced(task["target"])
        print("search_ref:",search_ref)
        print("salelink: ",search_ref['profile_link'])
        if search_ref['profile_link'] == "":
            print("no profile link")
            # result,error_message,count = [],"missing_profile_link",0
            processed_dict = {"processed":{"num_of_profiles":0, "result_main": {"error": "The profile link is empty","error_field":"profile_link","result": []  },"detailed_data":True}}
            processed_dict["last_updated"] = datetime.datetime.utcnow()
            self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)

            
            self.__update_current_task_status("processed_success", True)
            return True

        try:
            result, error_message,count = get_advanced_search_people_profile(self.driver,search_ref['profile_link'])
        except Exception as e:
            print("**********")
            print(e)     
            raise
        print("result:", result)

        if error_message == "":
            processed_dict = {"processed":{"num_of_profiles":count, "result_main": {"error": None,"error_field":None,"result": result  },"detailed_data":True}}
            processed_dict["last_updated"] = datetime.datetime.utcnow()
            self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)

            
            self.__update_current_task_status("processed_success", True)
            return True
            
        elif error_message =="check_profile":
            # processed_dict = {"processed":{"num_of_profiles":0, "result_main": {"error": "The profile didnt open","error_field":"search_link","result": result  },"detailed_data":True}}
            # processed_dict["last_updated"] = datetime.datetime.utcnow()
            # # self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)

            
            self.__update_current_task_status("processed_failure", True)
            return True
            
            
        else:
            if self.__check_if_worker_needs_validation(error_message):
                self.logger.critical("Worker needs validation")
#TODO: save the result?
                return "NEEDS_VALIDATION"
            self.__update_current_task_status("processed_failure", False)
            return False
        
        
        
        
    def __process_advanced_search_company(self, task: dict) -> bool:
        """
        Processes a task with category "advanced_search_company".

        Parameters
        ----------
        task : dict
            DESCRIPTION.

        Returns
        -------
        bool
            True if operation was successful, False otherwise.

        """

        if not self.logged_in:
            self.logger.warning(
            "Worker is not logged in."
            )
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()


        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )
    

      
        #extract the details of the search from firestore "ppl_search_advanced" collection
        search_ref = self.__fetch_advanced_search_companies(task["target"])
        print("search_ref:",search_ref)
        
        if "search_string" in search_ref.keys():
            search_string = search_ref["search_string"]
            search_ref['detailed_data']=False

        else:
            print("not executed yet")
            
        try:
            result, count, error_message = sales_company_search(self.driver,search_string,search_ref["detailed_data"],search_ref["max_tabs"],search_ref["return_counts_only"])
        except Exception as e:
            print("**********")
            print(e)     
            raise

        if search_ref['return_counts_only']==False:
            if error_message == "":
                if search_ref["detailed_data"]:
                    processed_dict = {"processed":{"num_of_profiles":count, "result_main": {"error": None,"error_field":None,"result": result,"search_link":search_string  },"detailed_data":search_ref["detailed_data"]}}
                    processed_dict["last_updated"] = datetime.datetime.utcnow()
                    self.advanced_company_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
    
                else:
                    processed_dict = {"processed":{"num_of_pages":count, "result_main": {"error": None,"error_field":None,"result": result,"search_link":search_string  },"detailed_data":search_ref["detailed_data"]}}
                    processed_dict["last_updated"] = datetime.datetime.utcnow()
                    self.advanced_company_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                self.__update_current_task_status("processed_success", True)
                return True
                
                
            else:
                if self.__check_if_worker_needs_validation(error_message):
                    self.logger.critical("Worker needs validation")
    #TODO: save the result?
                    return "NEEDS_VALIDATION"
                self.__update_current_task_status("processed_failure", False)
                # if result == []:
                # processed_dict = {"processed":{"num_of_profiles":0, "result_main": {"error": None,"error_field":"search_link does not exist","result": [],"search_link":search_string  },"detailed_data":search_ref["detailed_data"]}}
                # processed_dict["last_updated"] = datetime.datetime.utcnow()
                # self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
        
                return False
            
            
            
        elif search_ref['return_counts_only']==True:
            if error_message == "":
                
                processed_dict = {"processed":{"num_of_pages":count, "result_main": {"error": None,"error_field":None,"result": [] },"search_total_results":result['search_total_results'],"detailed_data":search_ref["detailed_data"]}}
                processed_dict["last_updated"] = datetime.datetime.utcnow()
                self.advanced_company_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)
                self.__update_current_task_status("processed_success", True)
                return True
                
                
            else:
                if self.__check_if_worker_needs_validation(error_message):
                    self.logger.critical("Worker needs validation")
    #TODO: save the result?
                    return "NEEDS_VALIDATION"
                self.__update_current_task_status("processed_failure", False)
                return False
            
            
        else:
            raise
            
            
    def __process_profile_search_companyl_advanced(self,task:dict) -> bool:
        """
        

        Parameters
        ----------
        task : dict
            DESCRIPTION.

        Returns
        -------
        bool
            True if operation was successful,False otherwise
            

        """
        if not self.logged_in:
            self.logger.warning(
            "Worker is not logged in."
            )
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()


        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )
    

      
        #extract the details of the search from firestore "ppl_search_advanced" collection
        search_ref = self.__fetch_company_search_advanced(task["target"])
        print("search_ref:",search_ref)
        print("salelink: ",search_ref['profile_link'])
        if search_ref['profile_link'] == "":
            print("no profile link")
            # result,error_message,count = [],"missing_profile_link",0
            processed_dict = {"processed":{"num_of_profiles":0, "result_main": {"error": "The profile link is empty","error_field":"profile_link","result": []  },"detailed_data":True}}
            processed_dict["last_updated"] = datetime.datetime.utcnow()
            self.advanced_company_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)

            
            self.__update_current_task_status("processed_success", True)
            return True

        try:
            result, error_message,count = get_advanced_search_company_profile(self.driver,search_ref['profile_link'])
        except Exception as e:
            print("**********")
            print(e)     
            raise
        print("result:", result)

        if error_message == "":
            processed_dict = {"processed":{"num_of_profiles":count, "result_main": {"error": None,"error_field":None,"result": result  },"detailed_data":True}}
            processed_dict["last_updated"] = datetime.datetime.utcnow()
            self.advanced_company_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)

            
            self.__update_current_task_status("processed_success", True)
            return True
            
        elif error_message =="check_profile":
            # processed_dict = {"processed":{"num_of_profiles":0, "result_main": {"error": "The profile didnt open","error_field":"search_link","result": result  },"detailed_data":True}}
            # processed_dict["last_updated"] = datetime.datetime.utcnow()
            # # self.advanced_ppl_search_ref.document(search_ref["id"]).set(processed_dict, merge=True)

            
            self.__update_current_task_status("processed_failure", True)
            return True
            
            
        else:
            if self.__check_if_worker_needs_validation(error_message):
                self.logger.critical("Worker needs validation")
#TODO: save the result?
                return "NEEDS_VALIDATION"
            self.__update_current_task_status("processed_failure", False)
            return False
        
    def __process_numerical_about(self,task:dict) -> bool:
        
        """
            Processes a task with category "about_numerical".
    
            Parameters
            ----------
            task : dict
                DESCRIPTION.
    
            Returns
            -------
            bool
                True if operation was successful, False otherwise.
    
            """

        if not self.logged_in:
            self.logger.warning(
            "Worker is not logged in."
            )
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()


        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )

        entity = self.__fetch_entity(task["target"])
         

        li_id = "https://www.linkedin.com/company/" + task["target"]

        new_numerical = getNumerical(self.driver,li_id)
        # new_about = getAbouts(self.driver, li_id)
        new_numerical['about'][0]['numericLink'] = li_id
        if not self.__check_if_worker_needs_validation(new_numerical):
            
            
            
            new_entity = self.__pre_process_about_numerical_data(new_numerical)
            # print("new_entity",new_entity)
            try:
                id_here = new_entity['about']['updated_Link'].split("/company/")[1].rstrip("/")
            except:
                try:
                    id_here = new_entity['about']['updated_Link'].split("/school/")[1].rstrip("/")
                except:
                    id_here = new_entity['about']['updated_Link'].split("/showcase/")[1].rstrip("/")
            
            
            # print("*****id*******:   ",id_here)
            if "/company/unavailable" in id_here:
                replace_linkedin_handle(task['target'], "")
            
            else:
                replace_linkedin_handle(task['target'],id_here)
            self.__store_num_about_in_firestore(new_entity)
            # self.__store_sb_in_firestore(new_entity,id_here)
            # if '/company/unavailable' not in new_entity['about']['updated_Link']:
                # print("")
                # self.__store_sb_in_supabase(new_entity, id_here)
                # self.__store_sb_in_firestore(new_entity, task['target'])
                # print("here")
            self.__update_current_task_status("processed_success", True)

            self.logger.info(
            f"Done processing task of ID: {task['id']} and of request ID: {task['request_id']}")
            return True
        else:
            self.logger.critical("Worker needs validation")
            return "NEEDS_VALIDATION"
        
        print("!@!@!@! shouldn't get here. but we are, for company task (returning False): ",task["id"])
        return False
            
    
    def __process_about(self, task: dict) -> bool:
        """
        Processes a task with category "about".

        Parameters
        ----------
        task : dict
            DESCRIPTION.

        Returns
        -------
        bool
            True if operation was successful, False otherwise.

        """

        if not self.logged_in:
            self.logger.warning(
            "Worker is not logged in."
            )
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()


        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )

        # print("task    ", task)
        entity = self.__fetch_entity(task["target"])
        
        # print("entity   ",entity)
        
        
        if entity is not None:
            
            
            if not self.__entity_needs_update(entity):
                self.logger.warning("No need to re-collect. Skipping")
                self.__update_current_task_status("processed_success", True)

                return True
            

        li_id = "https://www.linkedin.com/company/" + task["target"]

        new_about = getAbouts(self.driver, li_id)
        
        #print("new_about 1")
        #adjust the next next_about_update_date 
        request_type=task['request_id'].split("_")[2]
        #print("request_type:",request_type)
        # print(task)
        task_client_id=task['client']
        
        # print("new_about before processing:", new_about)
        # 
        
        if type(new_about) is dict:
            
            if task['ref'] == "DIRECT":
                entity["client"] = {}
                
                
            else:
               try:
                client = entity["client"]
                
                if "id" not in client.get(task_client_id, {}):
                    client[task_client_id] = {"id": ""}
            
                if request_type == 'subscription':
                    if "about_update_frequency_in_days" not in client.get(task_client_id, {}):
                        client[task_client_id]["about_update_frequency_in_days"] = 30
            
                    timedelta_x = client[task_client_id].get("about_update_frequency_in_days", 30)
                    next_about_update_date = datetime.datetime.strftime(
                        datetime.datetime.utcnow() + datetime.timedelta(days=int(timedelta_x)),
                        "%d-%m-%Y"
                    )
            
                    client[task_client_id]["next_about_update_date"] = next_about_update_date
            
                new_about.update({"client": client})
               except:
                   client ={}
        # print("new_about after client processing:", new_about)
        #print("new_about 2",new_about)
        if not self.__check_if_worker_needs_validation(new_about):
            
            
            
            new_entity = self.__pre_process_about_data(new_about)
            
            # print("new_entity before storing:", new_entity)
            # print("new_entity ", new_entity)
            
            self.__store_entity_in_firestore(new_entity)
            
            # if '/company/unavailable' not in new_entity['about']['updated_Link']:
            #     pass
            
                # self.__store_sb_in_supabase(new_entity, task['target'])
                # self.__store_sb_in_firestore(new_entity, task['target'])
            if task['ref'] == "DIRECT":
                self.__update_current_task_status("send_success", True)
            
            else:
                self.__update_current_task_status("processed_success", True)
                self.__update_sb_status_field()

            self.logger.info(
            f"Done processing task of ID: {task['id']} and of request ID: {task['request_id']}")
            return True
        else:
            self.logger.critical("Worker needs validation")
            return "NEEDS_VALIDATION"
        
        print("!@!@!@! shouldn't get here. but we are, for company task (returning False): ",task["id"])
        return False
            
#TODO: all
    def __process_25_months_employees(self, task: dict) -> bool:
        """
        Processes a task with category "25_months_employees".
        Parameters
        ----------
        task : dict
            DESCRIPTION.
        Returns
        -------
        bool
            True if operation was successful, False otherwise.
        """
        if not self.logged_in:
            self.logger.warning(
            "Worker is not logged in."
            )
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()


        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )


        search_ref = self.__fetch_25_months_employees(task["target"])
    
        # search_ref = self.__fetch_25_months_employees(task["target"])
        insg_ref = task['ref']
        print(insg_ref)
        li_id = "https://www.linkedin.com/company/" + task["target"]
        
        new_insights = get_25_months_employees(self.driver, li_id)
        print(new_insights)
        # Function to create the formatted data structure
        def create_formatted_data(insights_data):
            formatted_date_rd = datetime_class.strptime(insights_data['date_collected'], '%d-%b-%y').strftime('%Y-%m-%d')
            
            # Convert historical employee dates to (YYYY-MM) format
            formatted_history_RD = {
                datetime_class.strptime(list(entry.keys())[0], '%B %Y').strftime('%Y-%m'): list(entry.values())[0]
                for entry in insights_data['number_of_employees_history']
            }
            
            formate_date_dr = [
                {month: entry[month]} for entry in insights_data['number_of_employees_history'] for month in entry
            ]
            
            return {
                "insights": {
                    formatted_date_rd: formatted_history_RD,
                    'recent': {
                        "date_collected": datetime.datetime.utcnow().strftime('%d-%b-%y'),
                        "updated_Link": insights_data['updated_link'],
                        "client_doc_id": task['ref'],
                        "number_of_employees_history": formate_date_dr
                    }
                }
            }
    
        # Handle the case when there are no insights
        if new_insights['number_of_employees_history'] == [] and new_insights['updated_link'] != "https://www.linkedin.com/company/unavailable/":
            if new_insights['note'] == "noINSIGHTS":
                fs_data = create_formatted_data(new_insights)
                success = False
                
                try:
                    # Update the document directly
                    doc = self.entities_ref.document(search_ref["id"])
                    # First update the recent data
                    doc.set({
                        "insights.recent": fs_data['insights']['recent'],
                        "last_updated": datetime.datetime.utcnow()
                    }, merge=True)
                    # Then update the complete structure
                    doc.set(fs_data, merge=True)
                    success = True
                    
                except Exception as e:
                    self.logger.error(f"Error updating primary document: {e}")
                    try:
                        # Fallback to updating using target
                        doc = self.entities_ref.document(task["target"])
                        # Update recent data first
                        doc.set({
                            "insights.recent": fs_data['insights']['recent'],
                            "id": task["target"],
                            "last_updated": datetime.datetime.utcnow()
                        }, merge=True)
                        # Then update complete structure
                        doc.set(fs_data, merge=True)
                        success = True
                        
                    except Exception as e:
                        self.logger.error(f"Error updating fallback document: {e}")
                        success = False
                
                self.__update_current_task_status("processed_success" if success else "processed_failure", True)
                return True
                
            else:
                self.__update_current_task_status("processed_failure", True)
                return True
        
        # Handle the case when there are insights
        else:
            fs_data = create_formatted_data(new_insights)
            success = False
            
            try:
                # Update the document directly
                doc = self.entities_ref.document(search_ref["id"])
                # First update the recent data
                doc.set({
                    "insights": fs_data['insights']['recent'],
                    "last_updated": datetime.datetime.utcnow()
                }, merge=True)
                # Then update the complete structure
                doc.set(fs_data, merge=True)
                success = True
                
            except Exception as e:
                self.logger.error(f"Error updating primary document: {e}")
                try:
                    # Fallback to updating using target
                    doc = self.entities_ref.document(task["target"])
                    # Update recent data first
                    doc.set({
                        "insights": fs_data['insights']['recent'],
                        "id": task["target"],
                        "last_updated": datetime.datetime.utcnow()
                    }, merge=True)
                    # Then update complete structure
                    doc.set(fs_data, merge=True)
                    success = True
                    
                except Exception as e:
                    self.logger.error(f"Error updating fallback document: {e}")
                    success = False
            
            self.__update_current_task_status("processed_success" if success else "processed_failure", True)
            self.logger.info(f"Done processing task of ID: {task['id']} and of request ID: {task['request_id']}")
            return True
            
    
           
               
        
        
        
        
        """
        #print("new_about 1")
        #adjust the next next_about_update_date
        request_type=task['request_id'].split("_")[2]
        #print("request_type:",request_type)
        task_client_id=task['client']
        if type(new_about) is dict:
            client=entity["client"]
            if "id" not in entity["client"][task_client_id].keys():
                client[task_client_id]["id"]=""
            if request_type=='subscription':
                #temp ONLY
                if "about_update_frequency_in_days" not in entity["client"][task_client_id].keys():
                    entity["client"][task_client_id]["about_update_frequency_in_days"]=30
                #if "next_about_update_date" not in entity["client"][task_client_id].keys():
                #    entity["client"][task_client_id]["next_about_update_date"]=30
                timedelta_x=entity["client"][task_client_id]["about_update_frequency_in_days"]
                next_about_update_date=datetime.datetime.strftime(datetime.datetime.utcnow() + datetime.timedelta(days=int(timedelta_x)), "%d-%m-%Y")
                client[task_fclient_id]["next_about_update_date"]=next_about_update_date
            new_about.update({"client": client})
        #print("new_about 2",new_about)
        if not self.__check_if_worker_needs_validation(new_about):
            new_entity = self.__pre_process_about_data(new_about)
            self.__store_entity_in_firestore(new_entity)
            self.__update_current_task_status("processed_success", True)
            self.logger.info(
            f"Done processing task of ID: {task['id']} and of request ID: {task['request_id']}")
            return True
        else:
            self.logger.critical("Worker needs validation")
            return "NEEDS_VALIDATION"
        print("!@!@!@! shouldn't get here. but we are, for company task (returning False): ",task["id"])
        """
        return False



    def __process_sn_employees_movements(self,task:dict) -> bool:
        """
        Find past and current employees from advanced search if the task was last updated more than a given date. 
        It saves the result in a bucket if profile found. if profile not found it creates a task to get its html

        Parameters
        ----------
        task : dict
            DESCRIPTION.

        Returns
        -------
        bool
            DESCRIPTION.

        """
        if not self.logged_in:
            self.logger.warning(
            "Worker is not logged in."
            )
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()


        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )
        
        task_id = task['target'].split("__****__")[0]
        task_updated_date_nbre = int(task['target'].split("__****__")[1])
        query = self.db.collection_group('entities_all_employees_history').where('entity_id', '==', task_id)
        matching_docs = query.stream()
        for doc in matching_docs:
                doc_data = doc.to_dict()
                last_update_time = doc_data['last_updated']
                
        current_date_utc = datetime.datetime.now(datetime.timezone.utc)
        difference = current_date_utc - last_update_time
        is_more_than_30_days = difference.days > task_updated_date_nbre
        if is_more_than_30_days == True:
            self.__update_current_task_status("pre_success", True)
        else:
            get_sn_employees_movements(self.driver, task_id)
        
            self.__update_current_task_status("pre_success", True)
            return True
        
        
        
        
    def __process_profile_sales_html(self,task:dict) -> bool:
        """
        getting here the profiles in sales navigator to return them in html. 

        Parameters
        ----------
        task : dict
            DESCRIPTION.

        Returns
        -------
        bool
            DESCRIPTION.

        """
        if not self.logged_in:
            self.logger.warning(
            "Worker is not logged in."
            )
            self.logger.info(
                'Removing all remaining tasks in queue and setting status to "in_queue"'
            )
            self.__remove_all_remaining_tasks_in_queue()


        self.logger.debug(
            f"Processing task of ID: {task['id']} and of request ID: {task['request_id']}"
        )
    

      
        
        # task = {'client': 'TEST777', 'required_labels': ['search_people_advanced', 'productions'], 'status': 'in_process', 'id': 'p1_15052024_217638-848707_sn_ppl_ACwAAABd_FQBh3SCxLgGElCAzeKgfJkaeCe_Awg,NAME_SEARCH,vctO?', 'done_by': 'luciekhodeir@gmail.com', 'category': 'sn_ppl', 'target': 'ACwAAABd_FQBh3SCxLgGElCAzeKgfJkaeCe_Awg,NAME_SEARCH,vctO?',  'adhoc': True, 'ref': 'sn', 't_priority': 1, 'request_id': 'r1_15052024_adhoc_1'}
        saleID_link="https://www.linkedin.com/sales/lead/"+task['target']
        print(saleID_link)
        try:
            result= get_profile_sales_html(self.driver,saleID_link)
        except Exception as e:
            print("**********")
            print(e)     
            raise
        print("result:", result)

        if result == "worked":
            
            
            self.__update_current_task_status("download_success", True)
            return True
            
        elif result =="failed" or result == None:
           
            
            self.__update_current_task_status("processed_failure", True)
            return True
            
            
        else:
            if self.__check_if_worker_needs_validation(result):
                self.logger.critical("Worker needs validation")
#TODO: save the result?
                return "NEEDS_VALIDATION"
            self.__update_current_task_status("processed_failure", False)
            return False



    def __process_command(self, task):
        defined_categories = {
            "login": self.init_status_controller,
            "logout": self.init_status_controller,
        }

        status = False

        try:
            status = defined_categories[task["id"]](task["id"])
        except KeyError:
            self.logger.error(f"Command {task['id']} is not defined yet.")
            status = False
        finally:
            return status

    def __process_task(self, task):
        defined_categories = {
            "about": self.__process_about,
            "command": self.__process_command,
            "ppl": self.__process_ppl,
            "search_people": self.__process_search_ppl,
            "search_people_advanced": self.__process_advanced_search_ppl,
            "25_months_employees": self.__process_25_months_employees,
            "get_profile_search_people_advanced": self.__process_new_profile_search_ppl_advanced,
            "check_html": self.__check_html,
            "search_companies_advanced":self.__process_advanced_search_company,
            "get_profile_search_company_advanced": self.__process_profile_search_companyl_advanced,
            "sn_ppl":self.__process_profile_sales_html,
            "sn_employees_movements":self.__process_sn_employees_movements,
            "numerical_about":self.__process_numerical_about,
        }

        status = False

        try:
            if task["category"] != "command":
                self.__update_worker_status(
                    current_request=task["request_id"],
                    current_task=self.current_task_id,
                )
            t1 = time()    
            status = defined_categories[task["category"]](task)
            if status == False and task["category"] != "check_html" :
                print("Task execution failed with False")
                raise Exception("Task failed")
            elif status == False and task["category"] == "check_html":
                t2 = time()
                print("Task executed in ", t2-t1," seconds")
                status = True
                
            elif status == "NEEDS_VALIDATION":
                print("Task interrupted. Worker needs Validation!!")
            
            else:
                t2 = time()
                print("Task executed in ", t2-t1," seconds")
            
            return status
        
        except KeyError:
            self.logger.error(
                f"Task category \"{task['category']}\" is not defined yet."
            )
            status = False
            raise

        except Exception as e:
            print("##!##!##")
            print(e)
            status = False
            raise

    def __call__(self):
        unknown_error = False
        try:
            self.logger.info(f"Worker {self.email} started. Setting status to offline.")
            self.__update_worker_status("offline")

            while True:
                number_of_tasks = self.__check_for_new_tasks()

                if number_of_tasks > 0:
                    self.logger.info(
                        f"Fetched {number_of_tasks}, will start processing."
                    )

                    for task in self.tasks:
                        self.current_task_id = task["id"]
                        self.current_task = task
                        ret=self.__process_task(task)
                        
                        if ret == "NEEDS_VALIDATION":
                            self.__update_worker_status("NEEDS_VALIDATION")
                            self.__remove_all_remaining_tasks_in_queue() 
                            sys.exit(405)
                            
                            
                        #print("!! !! ret=",ret)
                        if ret != False:
                            self.__remove_current_task_from_firestore_queue()

        except KeyboardInterrupt:
            self.logger.info(
                f"Worker {self.email} received k_eyboard interrupt. Exiting gracefully."
            )
        except Exception:
            self.logger.exception("Uncaught exception raised. See below:")
            self.logger.exception((logging.traceback.format_exc()))
            unknown_error = True

        finally:
            self.exception_handler(unknown_error)

    def exception_handler(self, *args):
        self.init_status_controller("logout")
        self.__update_worker_status("off")
    
        self.logger.info(
            'Removing all remaining tasks in queue and setting status to "in_queue"'
        )
        self.__remove_all_remaining_tasks_in_queue()
        sys.exit()
        
if __name__ == "__main__":
    try:
        workerConfigPath = sys.argv[1]
    except IndexError:
        WORKERS_PATH = './workers/'
        files = os.listdir(WORKERS_PATH)
        workerConfigPath = WORKERS_PATH + files[0]
    worker = Worker(workerConfigPath)
    signal.signal(signal.SIGINT, worker.exception_handler)
    worker()
    
