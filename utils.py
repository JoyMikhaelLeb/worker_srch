# -*- coding: utf-8 -*-
"""
Created on Tue Oct 26 12:53:16 2021

@author: charb
"""
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException,TimeoutException

import random
from selenium import webdriver
import time
from random import randint
import firebase_admin
from firebase_admin import credentials, firestore
import datetime
from google.cloud.firestore_v1 import Increment
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import NoSuchElementException
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
import os
import re
import emoji
from datetime import datetime,date
from selenium.webdriver.common.by import By


    
def getPass(acct):
    """
    Gets password from firestore of specified acct
    """
    if not firebase_admin._apps:
        cred = credentials.Certificate("spherical-list-284723-216944ab15f1.json")
        default_app = firebase_admin.initialize_app(cred)

    db = firestore.client()
    doc = db.collection(u"workers").document(acct).get()
    worker = doc.to_dict()
    return worker["password"]


def getVerificationCode(acct):
    """
    Gets verification code from firestore on specified acct
    """
    
    if not firebase_admin._apps:
        cred = credentials.Certificate("spherical-list-284723-216944ab15f1.json")
        default_app = firebase_admin.initialize_app(cred)

    db = firestore.client()
    doc = db.collection(u"workers").document(acct).get()
    worker = doc.to_dict()
    return worker["verification_code"]


def wait_for_verification_code(acct, timeout=600, check_interval=10):
    """
    Waits for the verification code to appear in Firestore.

    Parameters:
        acct (str): The email/account for which to fetch the verification code.
        timeout (int): Maximum wait time in seconds (default: 10 minutes).
        check_interval (int): How often to check Firestore (default: 10 seconds).
    
    Returns:
        str: The verification code if found, otherwise None.
    """
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        code = getVerificationCode(acct)
        if code!="":  # Ensure a valid code exists
            print(f"Verification code received: {code}")
            return code
        print("Waiting for verification code...")
        time.sleep(check_interval)
    
    print("Timeout reached. No verification code found.")
    return None


def linkedin_logout(driver):
    driver.close()


def linkedin_login(username, password, headless=False):
    url = "https://www.linkedin.com/login"
    chrome_options = webdriver.ChromeOptions()

    if not headless:
        prefs = {"profile.default_content_setting_values.notifications": 2}
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--incognito")
        
        chrome_options.add_argument("--start-maximized")
        # print("removed incognito")
    else:
        chrome_options.add_argument('--headless=new')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        chrome_options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
        chrome_options.add_experimental_option('useAutomationExtension', False)

    try:
        driver = webdriver.Chrome(executable_path=ChromeDriverManager().install(), chrome_options=chrome_options)

        if headless:
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            })
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    except WebDriverException as e:
        print(f"Error: {e}")
        raise

    if headless:
        print("Browser ready in headless mode")
    
    driver.get(url)

    driver.find_element_by_id("username").send_keys(username)

    driver.find_element_by_id("password").send_keys(password)
    
    try:
        label = driver.find_element(By.CSS_SELECTOR, "label[for='rememberMeOptIn-checkbox']")
        label.click()
        print("unchecked-rememberme")
    except:
        pass
    
    driver.find_element_by_xpath("//div[contains(@class,'login__form_action_container')]//*[contains(@aria-label, 'Sign in')]").click()
    time.sleep(2 + randint(1, 3))
    
    current_url = driver.current_url
    
    if 'linkedin.com/checkpoint/challenge/' in current_url:
        
        
        elements = driver.find_elements_by_xpath("//button[normalize-space(text())='Agree to comply']")
        if len(elements) > 0:
            print("Element found! Clicking it...")
            elements[0].click()
            time.sleep(2 + randint(1, 3))
        else:
            print("Element not found. Skipping...")
            print("Verification required. Waiting for code...")
            verification_code = wait_for_verification_code(username)
            if verification_code!="":
                try:
                    verification_input = driver.find_element_by_id("input__phone_verification_pin")
                    verification_input.send_keys(verification_code)
                    time.sleep(1)
                    driver.find_element(By.XPATH, "//button[contains(@aria-label, 'Submit')]").click()
                    print("Verification code submitted.")
                    time.sleep(5)
                except Exception as e:
                    print(f"Error entering verification code: {e}")
                    driver.quit()
                    return None
            else:
                print("No verification code received. Exiting.")
                driver.quit()
                return None
        
    return driver


def getNumberOfEmployees(driver):
    try:
        numberOfEmployees = driver.find_element_by_xpath(
            "//span[@class='t-normal t-black--light link-without-visited-state link-without-hover-state']"
            ).text
        if 'employee' in numberOfEmployees:
            numberOfEmployees = numberOfEmployees.split(" employee")[0]
        
        if 'all' in numberOfEmployees:
            numberOfEmployees = numberOfEmployees.split("all ")[1]
        
        try:
            numberOfEmployees = int(numberOfEmployees.replace(",",""))

        except:
            try:
                numberOfEmployees = int(numberOfEmployees)
            except:
                if '-' in numberOfEmployees:
                    driver.find_element_by_xpath("//span[@class='t-normal t-black--light link-without-visited-state link-without-hover-state']").click()
                    time.sleep(random.uniform(5, 10))
                    try:
                        numberOfEmployees = driver.find_elements_by_xpath("//div[@class='search-results-container']//div/h2")[0].text.split("result")[0]
                        if 'About' in numberOfEmployees:
                            numberOfEmployees = numberOfEmployees.split("About ")[1]
                        try:
                            numberOfEmployees= int(numberOfEmployees)
                        except:
                            numberOfEmployees = int(numberOfEmployees.replace(",",""))
                    except:
                        numberOfEmployees = 0
    except:
        try:
            numberOfEmployees=driver.find_element_by_xpath("//span[contains(@class, 't-normal t-black--light link-without-visited-state link-without-hover-state')]").text
            numberOfEmployees = numberOfEmployees.split(" employee")[0]
        
            if 'all' in numberOfEmployees:
                numberOfEmployees = numberOfEmployees.split("all ")[1]
            else:
                try:
                    numberOfEmployees= int(numberOfEmployees)
                except:
                    numberOfEmployees = int(numberOfEmployees.replace(",",""))
            try:
                numberOfEmployees= int(numberOfEmployees)
            except:
                numberOfEmployees = int(numberOfEmployees.replace(",",""))
        except:
            numberOfEmployees = 0
                
    driver.execute_script("window.history.go(-1)")
    return numberOfEmployees


def clean_url(pageID):
    """Cleans LinkedIn company URLs by removing unnecessary fragments."""
    fragments_to_remove = ["/about", "/mycompany", "/admin", "?originalSubdomain"]
    for fragment in fragments_to_remove:
        pageID = pageID.replace(fragment, "")
    return pageID.rstrip("/")


def extract_numerical_from_url(current_url):
    """Extracts the numerical ID from the LinkedIn company URL."""
    try:
        if "%5B%22" in current_url and "%22%5D" in current_url:
            return current_url.split("%5B%22")[1].split("%22%5D")[0]
        elif "%5B" in current_url and "%5D" in current_url:
            return current_url.split("%5B")[1].split("%5D")[0]
        elif "/company/" in current_url:
            return current_url.split("/company/")[1].rstrip("/")
        if '%22' in current_url:
                return current_url.split("%22")[0]
    except IndexError:
        raise ValueError("Failed to extract numerical ID from URL")
    return None


def update_firestore_with_conditional_created(db, collection_name, document_id, data):
    """
    Updates a Firestore document with conditional 'created' field.
    
    Parameters:
    - db: Firestore client
    - collection_name: Name of the collection
    - document_id: Document ID
    - data: Dictionary of data to update
    
    The function automatically:
    - Adds 'last_updated' field with current timestamp
    - Adds 'created' field only if the document doesn't exist
    """
    doc_ref = db.collection(collection_name).document(document_id)
    doc = doc_ref.get()
    document_exists = doc.exists
    
    # Always add last_updated
    data['last_updated'] = datetime.utcnow()
    
    # Only add created if document doesn't exist
    if not document_exists:
        data['created'] = datetime.utcnow()
    
    doc_ref.set(data, merge=True)
    
    return document_exists  # Return whether doc existed before update

def get_numericalID(driver, db, pageID):
    # Clean the pageID
    if '/about' in pageID:
        pageID = pageID.replace("/about", "")
    if '/' in pageID:
        pageID = pageID.rstrip("/")
    if '/mycompany' in pageID:
        pageID = pageID.replace("/mycompany", "")
    if '/admin' in pageID:
        pageID = pageID.replace("/admin", "")
    if '?originalSubdomain' in pageID:
        pageID = pageID.split('?originalSubdomain')[0]

    # Check Firestore first for existing numerical ID
    docu_ref = db.collection(u'entities').document(pageID)
    doc = docu_ref.get()
    
    if doc.exists:
        doc_out = doc.to_dict()
        try:
            numericLink = doc_out['about']['numericLink']
            if numericLink != "":
                if '%' in numericLink:
                    numericLink = numericLink.split("%")[0]
                try:
                    target_numerical = numericLink.split("https://www.linkedin.com/company/")[1]
                except:
                    target_numerical = numericLink.split("http://www.linkedin.com/company/")[1]
                return target_numerical
        except KeyError:
            pass

    # Navigate to LinkedIn page only once
    target_link = "https://www.linkedin.com/company/" + pageID + '/'
    
    try:
        driver.get(target_link)
        time.sleep(random.uniform(7, 20))
    except TimeoutException:
        print("Here's the timeout in experience. Refreshing...")
        driver.refresh()
        time.sleep(random.uniform(7, 20))
        driver.get(target_link)
        time.sleep(random.uniform(7, 20))

    # Get current URL and clean it
    current_target = driver.current_url
    
    if '/about' in current_target:
        current_target = current_target.replace("/about", "")
    if '/' in current_target:
        current_target = current_target.rstrip("/")
    if '/mycompany' in current_target:
        current_target = current_target.replace("/mycompany", "")
    if '/admin' in current_target:
        current_target = current_target.replace("/admin", "")
    if '?originalSubdomain' in current_target:
        current_target = current_target.split('?originalSubdomain')[0]

    # Check for error pages
    if current_target == "https://www.linkedin.com/404/":
        return "page_doesnt_exist"
    if any(substring in current_target for substring in ["/checkpoint/challenge", "login?session_redirect", "authwall?trk"]):
        return "security_check"

    # Check for "Something went wrong" message
    try:
        smtg_wrong = driver.find_element_by_xpath("//h2[contains(@class,'artdeco-empty-state__headline')]").text
        if 'Something went wrong' in smtg_wrong:
            return "page_doesnt_exist"
    except:
        pass

    # Try to extract numerical ID from current page
    def extract_numerical_id():
        try:
            # Click on the element to reveal numerical ID in URL
            driver.find_element_by_xpath("//span[@class='t-normal t-black--light link-without-visited-state link-without-hover-state']").click()
            time.sleep(random.uniform(4, 10))
            
            current = driver.current_url
            print(f"Current URL after click: {current}")
            
            # Try multiple URL patterns to extract numerical ID
            numerical = None
            
            if "%5B%22" in current and "%22%5D" in current:
                numerical = current.split("%5B%22")[1].split("%22%5D")[0]
            elif "%5B" in current and "%5D" in current:
                numerical = current.split("%5B")[1].split("%5D")[0]
            elif "/company/" in current:
                # Try to extract directly from company URL
                parts = current.split("/company/")[1].rstrip('/')
                if parts and parts != pageID:  # Make sure it's different from original pageID
                    numerical = parts
            
            if not numerical:
                raise ValueError("Could not extract numerical ID from URL")
            
            # Clean the numerical ID
            if '%22' in numerical:
                numerical = numerical.split("%22")[0]
            if '%' in numerical:
                numerical = numerical.split("%")[0]
            
            # Validate numerical ID
            if not numerical or numerical == pageID:
                raise ValueError("Invalid numerical ID extracted")
            
            return numerical
            
        except Exception as e:
            print(f"Error extracting numerical ID: {e}")
            return None

    # Get number of employees to determine if page is valid
    number_of_employees = getNumberOfEmployees(driver)
    
    if number_of_employees == 0:
        # Try to extract numerical ID anyway
        numerical = extract_numerical_id()
        if not numerical:
            return 0
    else:
        # Page has employees, extract numerical ID
        numerical = extract_numerical_id()
        if not numerical:
            return 0

    # Save numerical ID to Firestore
    try:
        about_data = {
            'about': {
                'updated_Link': f'https://www.linkedin.com/company/{pageID}/',
                'numericLink': f'https://www.linkedin.com/company/{numerical}',
                'date_collected': date.today().strftime("%d-%b-%y")
            },
            'id': pageID,
            'parallel_number': randint(1, 10)
        }
        
        update_firestore_with_conditional_created(db, 'entities', pageID, about_data)
        print(f"***Updated with numerical ID: {numerical}***")
        return numerical
        
    except Exception as e:
        print(f"Error saving to Firestore: {e}")
        return numerical if numerical else 0
def get_numericalID___old_way(driver, db, pageID):
    if '/about' in pageID:
        pageID = pageID.replace("/about", "")
    if '/' in pageID:
        pageID = pageID.rstrip("/")
    if '/mycompany' in pageID:
        pageID = pageID.replace("/mycompany", "")
    if '/admin' in pageID:
        pageID = pageID.replace("/admin", "")
    if '?originalSubdomain' in pageID:
        pageID = pageID.split('?originalSubdomain')[0]

    docu_ref = db.collection(u'entities').document(pageID)
    doc = docu_ref.get()
    
    # Check if document exists
    document_exists = doc.exists

    if document_exists:
        doc_out = doc.to_dict()
        try:  # numerical_in_firestore
            numericLink = doc_out['about']['numericLink']
            if numericLink!= "":
                if '%' in numericLink:
                    numericLink = numericLink.split("%")[0]
                try:
                    target_numerical = numericLink.split("https://www.linkedin.com/company/")[1]
                except:
                    target_numerical = numericLink.split("http://www.linkedin.com/company/")[1]
                return target_numerical
        except KeyError:  # doc exists but no numerical
            pass

    # If document does not exist or numerical ID is missing
    target_link = "https://www.linkedin.com/company/" + pageID + '/'
    
    try:
        driver.get(target_link)
        time.sleep(random.uniform(7, 20))
    except TimeoutException:
        print("Here's the timeout in experience. Refreshing...")
        driver.refresh()
        time.sleep(random.uniform(7, 20))
        driver.get(target_link)
        time.sleep(random.uniform(7, 20))

    current_target = driver.current_url
    
    if '/about' in current_target:
        current_target = current_target.replace("/about", "")
    if '/' in current_target:
        current_target = current_target.rstrip("/")
    if '/mycompany' in current_target:
        current_target = current_target.replace("/mycompany", "")
    if '/admin' in current_target:
        current_target = current_target.replace("/admin", "")
    if '?originalSubdomain' in current_target:
        current_target = target_link.split('?originalSubdomain')[0]

    if current_target == "https://www.linkedin.com/404/":
        return "page_doesnt_exist"
    if any(substring in current_target for substring in ["/checkpoint/challenge", "login?session_redirect", "authwall?trk"]):
        return "security_check"

    try:
        smtg_wrong = driver.find_element_by_xpath("//h2[contains(@class,'artdeco-empty-state__headline')]").text
        if 'Something went wrong' in smtg_wrong:
            return "page_doesnt_exist"
    except:
        pass

    number_of_employees = getNumberOfEmployees(driver)
    if number_of_employees == 0:
        try:
            driver.get(target_link)
            time.sleep(random.uniform(4, 7))
            driver.find_element_by_xpath("//span[@class='t-normal t-black--light link-without-visited-state link-without-hover-state']").click()
            time.sleep(random.uniform(4, 10))
            current = driver.current_url
            try:
                numerical = current.split("%5B%22")[1].split("%22%5D")[0]
            except IndexError:
                try:
                    numerical = current.split("%5B")[1].split("%5D")[0]
                except IndexError:
                    raise ValueError("Failed to extract numerical ID")
                    
            if '%22' in numerical:
                numerical = numerical.split("%22")[0]
            
            # Save numerical ID to Firestore - using helper function
            about_data = {
                'about': {
                    'updated_Link': f'https://www.linkedin.com/company/{pageID}/',
                    'numericLink': f'https://www.linkedin.com/company/{numerical}',
                    'date_collected': date.today().strftime("%d-%b-%y")
                },
                'id': pageID,
                'parallel_number': randint(1, 10)
            }
            
            update_firestore_with_conditional_created(db, 'entities', pageID, about_data)
            print("***updated***")
            return numerical
        except Exception as e:
            print(f"Error: {e}")
            return 0
    
    if number_of_employees != 0:
        driver.get(target_link)
        try:
            try:
                driver.find_element_by_xpath("//span[@class='t-normal t-black--light link-without-visited-state link-without-hover-state']").click()
                time.sleep(random.uniform(7, 20))
            except:
                numerical = 0

            current = driver.current_url
            print(f"Current URL: {current}")  # Debug print
            
            # Try multiple URL patterns
            if "%5B%22" in current and "%22%5D" in current:
                numerical = current.split("%5B%22")[1].split("%22%5D")[0]
            elif "%5B" in current and "%5D" in current:
                numerical = current.split("%5B")[1].split("%5D")[0]
            elif "/company/" in current:
                # Try to extract directly from company URL
                numerical = current.split("/company/")[1].rstrip('/')
            else:
                raise ValueError("URL format not recognized")
            
            if '%22' in numerical:
                numerical = numerical.split("%22")[0]
            # Validate numerical ID
            if not numerical:
                raise ValueError("Empty numerical ID extracted")
            
            if '%' in numerical:
                numerical = numerical.split("%")[0]
            
            # Save to Firestore - using helper function
            about_data = {
                'about': {
                    'updated_Link': f'https://www.linkedin.com/company/{pageID}/',
                    'numericLink': f'https://www.linkedin.com/company/{numerical}',
                    'date_collected': date.today().strftime("%d-%b-%y")
                },
                'id': pageID,
                'parallel_number': randint(1, 10)
            }
            
            update_firestore_with_conditional_created(db, 'entities', pageID, about_data)
            print(f"Updated with numerical ID: {numerical}")
            return numerical
            
        except Exception as e:
            print(f"Error: {e}")
            return 0

    return 0


def remove_emojis(s):
    return ''.join(c for c in s if c not in emoji.UNICODE_EMOJI['en']).strip()


remove_list=['MBA', 'PhD', 'CFO', 'CFA', 'MD', 'Ph.D', 'CPA', 'CTE', 'PMP', 'CSM', 'MCIPD','DR', 'SSM', 'HE','SHE','HIM','HER','HIS', 'HERS']
replace_by_space_list=['/','.','(',')']
custom_replace_list=['Dipl.-Math.']


def clean_name(name_in):
    #remove non-ascii
    name_out = remove_emojis(name_in)
    #custom_replace
    for custom_replace_item in  custom_replace_list:
        name_out=name_out.replace(custom_replace_item,"")
    name_out=name_out.strip()
    
    #basic replacements
    for replace_by_space_item in replace_by_space_list:
        name_out= name_out.replace(replace_by_space_item," ")
    name_out=name_out.title().split(",")[0]
    
    remove_list_title = [x.title() for x in remove_list]
    
    name_out_array_temp=name_out.split()
    name_out_array=[x.strip() for x in name_out_array_temp if x not in remove_list_title]
    
    name_out=" ".join(name_out_array)
    
    return name_out


def remove_more_emoji(name_in):
    emoji_pattern = re.compile(
        u'(\U0001F1F2\U0001F1F4)|'       # Macau flag
        u'([\U0001F1E6-\U0001F1FF]{2})|' # flags
        u'([\U0001F600-\U0001F64F])'     # emoticons
        "+", flags=re.UNICODE)

    return emoji_pattern.sub('', name_in).rstrip(" ")


def getLink(driver,link):
    search_link = "https://www.linkedin.com/company/"+link
    try:
        driver.get(search_link)
        time.sleep(random.uniform(7, 20))
    except TimeoutException:
        print("Here's the timeout in experience. Refreshing...")
        driver.refresh()
        time.sleep(random.uniform(7, 20))
        driver.get(search_link)
        time.sleep(random.uniform(7, 20))
        
    current_link = driver.current_url
        
    if 'company/unavailable' in current_link or '/about/':
        search_click = "unavailable"
    else:
        search_click = "no problem"
        
    return search_click


def get_name_and_numericalID(driver, db, cname):
    links = "https://www.linkedin.com/company/"+cname+"/"
    coll_ref = db.collection("entities")
    
    try:
        docs = [
            snapshot
            for snapshot in coll_ref.limit(250)
            .where("id", "==", cname).stream()
        ]
    except:
        docs = [
            snapshot
            for snapshot in coll_ref.limit(250)
            .where("about.updated_Link", "==", links).stream()
        ]
    
    if len(docs) != 0:
        if len(docs) > 1:
            docs = [
                snapshot
                for snapshot in coll_ref.limit(250)
                .where("id", "==", cname).stream()
            ]
            
        for doc in docs:
            docum_out = doc.to_dict()

            try:
                company_numeric_id = docum_out['about']['numericLink']
                if company_numeric_id == '':
                    company_numeric_id = 0
                elif docum_out['about']['updated_Link']=='https://www.linkedin.com/company/unavailable/' and docum_out['about']['numericLink'] == 0:
                    company_numeric_id = 0
                    
                if 'https://www.linkedin.com/company' in company_numeric_id or 'http://www.linkedin.com/company' in company_numeric_id:
                    if '%' in company_numeric_id:
                        company_numeric_id = company_numeric_id.split("%")[0]
                    company_numeric_id = company_numeric_id.split("/company/")[1]
                        
                company_name = docum_out['about']['Name']
                
            except:
                try:
                    driver.get(links)
                    time.sleep(random.uniform(5, 10))
                except TimeoutException:
                    print("Here's the timeout in experience. Refreshing...")
                    driver.refresh()
                    time.sleep(random.uniform(5, 10))
                    driver.get(links)
                    time.sleep(random.uniform(5, 10))
                
                try:
                    company_name = driver.find_element_by_xpath("//div[@class='block mt4']//h1").text
                except:
                    company_name = ""
                    
                print(company_name)
                
                try:
                    try:
                        try:
                            driver.find_element_by_xpath("//span[@class='t-normal t-black--light link-without-visited-state link-without-hover-state']").click()
                            time.sleep(7)
                            continue_flag = True
                        except:
                            company_numeric_id = 0
                            continue_flag = False
                        
                        if continue_flag == True:
                            current = driver.current_url
                            print(f"Current URL: {current}")  # Debug print
                            
                            # Try multiple URL patterns
                            if "%5B%22" in current and "%22%5D" in current:
                                company_numeric_id = current.split("%5B%22")[1].split("%22%5D")[0]
                            elif "%5B" in current and "%5D" in current:
                                company_numeric_id = current.split("%5B")[1].split("%5D")[0]
                            elif "/company/" in current:
                                company_numeric_id = current.split("/company/")[1].rstrip('/')
                            else:
                                raise ValueError("URL format not recognized")
                            
                            # Validate numerical ID
                            if not company_numeric_id:
                                raise ValueError("Empty company_numeric_id ID extracted")
                
                            if '%' in company_numeric_id:
                                company_numeric_id = company_numeric_id.split("%")[0]
                            
                            # Save to Firestore - using helper function
                            about_data = {
                                'about': {
                                    'updated_Link': f'https://www.linkedin.com/company/{cname}/',
                                    'numericLink': f'https://www.linkedin.com/company/{company_numeric_id}',
                                    'date_collected': date.today().strftime("%d-%b-%y")
                                },
                                'id': cname,
                                'parallel_number': randint(1, 10)
                            }
                            
                            update_firestore_with_conditional_created(db, 'entities', cname, about_data)
                            print(f"Updated with numerical ID: {company_numeric_id}")
                   
                    except:
                        raise
                        company_numeric_id = 0
                except:
                    raise
                    company_numeric_id = 0

    if len(docs) == 0:
        try:
            driver.get(links)
            time.sleep(random.uniform(5, 10))
        except TimeoutException:
            print("Here's the timeout in experience. Refreshing...")
            driver.refresh()
            time.sleep(random.uniform(5, 10))
            driver.get(links)
            time.sleep(random.uniform(5, 10))
        try:
            company_name = driver.find_element_by_xpath("//div[@class='block mt4']//h1").text
        except:
            company_name = ""
            
        print(company_name)
        try:
            try:
                try:
                    driver.find_element_by_xpath("//span[@class='t-normal t-black--light link-without-visited-state link-without-hover-state']").click()
                    time.sleep(5)
                    continue_flag = True
                except:
                    company_numeric_id = 0
                    continue_flag = False
                    
                if continue_flag == True:
                    current = driver.current_url
                    print(f"Current URL: {current}")  # Debug print
                    
                    # Try multiple URL patterns
                    if "%5B%22" in current and "%22%5D" in current:
                        company_numeric_id = current.split("%5B%22")[1].split("%22%5D")[0]
                    elif "%5B" in current and "%5D" in current:
                        company_numeric_id = current.split("%5B")[1].split("%5D")[0]
                    elif "/company/" in current:
                        company_numeric_id = current.split("/company/")[1].rstrip('/')
                    else:
                        raise ValueError("URL format not recognized")
        
                    # Validate numerical ID
                    if not company_numeric_id:
                        raise ValueError("Empty company_numeric_id ID extracted")
        
                    if '%' in company_numeric_id:
                        company_numeric_id = company_numeric_id.split("%")[0]
                    
                    # Save to Firestore - using helper function (new document)
                    about_data = {
                        'about': {
                            'updated_Link': f'https://www.linkedin.com/company/{cname}/',
                            'numericLink': f'https://www.linkedin.com/company/{company_numeric_id}',
                            'date_collected': date.today().strftime("%d-%b-%y")
                        },
                        'id': cname,
                        'parallel_number': randint(1, 10)
                    }
                    
                    update_firestore_with_conditional_created(db, 'entities', cname, about_data)
                    print(f"Updated with numerical ID: {company_numeric_id}")
                   
            except:
                raise
                company_numeric_id = 0
        except:
            raise
            company_numeric_id = 0
            
    return company_numeric_id, company_name


def increment_counter(db, client_id: str, category: str):
    doc_ref = db.collection("clients")
    doc = doc_ref.get()
    for ind, i in enumerate(doc):
        if doc_ref.document(i.id).get().to_dict()["client_id"] == client_id:
            client_ref = i.id
            break

    date = datetime.datetime.strftime(datetime.datetime.now(), "%m_%y")

    if not f"about_{category}" in doc_ref.document(client_ref).get().to_dict().keys():
        doc_ref.document(client_ref).update({f"about_{category}": {date: 1}})
    else:
        doc_ref.document(client_ref).set(
            {f"about_{category}": {date: Increment(1)}},
            merge = True
        )


def page_doesnt_exist_check(driver):
    try:
        driver.find_element_by_xpath("//section//h1[text()[contains(.,'Something went wrong')]]")    
        return True
    except:
        return False
    

def needs_validation_check(driver):
    try:
        driver.find_element_by_xpath("//main[@class='app__content']//h1[text()[contains(.,'do a quick verification')]]")    
        return True
    except:
        return False


def update_worker_status(
    db,
    user_id: str,
    status: str = None,
    current_request: str = "NA",
    current_task: str = "NA",
):
    '''
    Parameters
    ----------
    db: firestore.client
        Database instance
    user_id : str
        Email address.
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

    '''
    if status is not None:
        process = True
        if current_request == "NA" or current_task == "NA":
            doc = {"id": user_id, "status": status, "since": datetime.datetime.now()}

        else:
            doc = {
                "current_request_processing": current_request,
                "current_task_processing": current_task,
                "id": user_id,
                "status": status,
                "since": datetime.datetime.now(),
            }

    else:
        if not current_request == "NA" or not current_task == "NA":
            doc = {
                "current_request_processing": current_request,
                "current_task_processing": current_task,
                "id": user_id,
            }
            process = True
        else:
            process = False

    if process:
        db.collection("dashboards").document("logs").collection("workers").document(
            user_id
        ).set(doc, merge=True)
        

def check_worker_current_status(db, user_id: str):
    worker_doc = db.collection("dashboards").document("logs").collection("workers").document(user_id).get()

    if worker_doc is None:
        return "offline"
    
    if type(worker_doc) is list and len(worker_doc) == 0:
        return "offline"
    
    try:
        status = worker_doc.to_dict()["status"]
        return status
    except:
        return "offline"
