"""
Supabase Table Interface

This module provides an interface for interacting with a Supabase table named 'founding_dates'.
It includes functions for adding, updating, removing, and querying rows in the table.

The table is assumed to have the following structure:
- li_id (string): A unique identifier for each row
- main_founding_year (integer): The main founding year of the company
- oldest_founder_founding_year (integer): The founding year of the oldest founder
- oldest_founder_founding_month (integer): The founding month of the oldest founder
- created_at (datetime): The creation date of the row (automatically managed by Supabase)
- updated_at (datetime): The last update date of the row (automatically managed by Supabase)

Requirements:
- supabase-py library
- Environment variables set for SUPABASE_URL, SUPABASE_KEY, SUPABASE_USER_EMAIL, SUPABASE_USER_PASSWORD
"""

import os
import re
import json
from typing import Dict, List, Optional, Union, Any
import logging

from datetime import datetime
from supabase import create_client, Client
from pydantic import BaseModel

# Import the Secret Manager client library.
from google.cloud import secretmanager
from datetime import timedelta

from typing import Literal, Set, Tuple


# GCP project in which to store secrets in Secret Manager.
project_id = "spherical-list-284723"

# Create the Secret Manager client.
client = secretmanager.SecretManagerServiceClient()

# Build the resource name of the parent project.
parent = f"projects/{project_id}"


# Add these lines for authentication
email = "joy@randeeco.com"

secret_id="supabase_pass"   
version_id="1" 
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
# Build the resource name of the secret.
response = client.access_secret_version(request={"name": name})

password=response.payload.data.decode("UTF-8")

# Initialize Supabase client
#url: str = os.environ.get("SUPABASE_URL")
#key: str = os.environ.get("SUPABASE_KEY")
url="https://qogbgxtydyhvmaveoxys.supabase.co"

secret_id="supabase_key"   
version_id="2" 
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
# Build the resource name of the secret.
response = client.access_secret_version(request={"name": name})

key=response.payload.data.decode("UTF-8")

supabase: Client = create_client(url, key)



# Authenticate (assuming you have a way to get these credentials)
auth_response = supabase.auth.sign_in_with_password({"email": email, "password": password})

if auth_response.user is None:
    raise Exception("Authentication failed")


def connect_to_supabase() -> Client:
    """Connect to Supabase using GCP secrets"""
    try:
        project_id = "spherical-list-284723"
        client = secretmanager.SecretManagerServiceClient()
        parent = f"projects/{project_id}"
        email = "joy@randeeco.com"

        # Get password
        secret_id = "supabase_pass"
        version_id = "1"
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        password = response.payload.data.decode("UTF-8")

        # Get API key
        url = "https://qogbgxtydyhvmaveoxys.supabase.co"
        secret_id = "supabase_key"
        version_id = "2"
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        key = response.payload.data.decode("UTF-8")

        supabase: Client = create_client(url, key)
        auth_response = supabase.auth.sign_in_with_password({"email": email, "password": password})

        if auth_response.user is None:
            return -2
    except Exception as e:
        print(f"Error in connect_to_supabase: {str(e)}")
        return -1
    return supabase


def validate_year(year: Optional[int]) -> bool:
    """
    Validate if the given year is None or within the range 1800 to the current year.
    
    Args:
        year (Optional[int]): The year to validate.
    
    Returns:
        bool: True if the year is valid, False otherwise.
    """
    current_year = datetime.utcnow().year
    return year is None or (1800 <= year <= current_year)

def validate_month(month: Optional[int]) -> bool:
    """
    Validate if the given month is None or within the range 1-12.
    
    Args:
        month (Optional[int]): The month to validate.
    
    Returns:
        bool: True if the month is valid, False otherwise.
    """
    return month is None or 1 <= month <= 12

def validate_date_not_future(year: int, month: int) -> bool:
    """
    Validate if the given year and month combination is not in the future.
    
    Args:
        year (int): The year to validate.
        month (int): The month to validate.
    
    Returns:
        bool: True if the date is not in the future, False otherwise.
    """
    current_date = datetime.utcnow()
    return (year < current_date.year) or (year == current_date.year and month <= current_date.month)

def add_new_row(
    li_id: str,
    main_founding_year: Optional[int] = None,
    oldest_founder_founding_year: Optional[int] = None,
    oldest_founder_founding_month: Optional[int] = None
) -> Dict:
    """
    Add a new row to the founding_dates table.
    
    Args:
        li_id (str): Unique identifier for the row.
        main_founding_year (Optional[int]): Main founding year of the company.
        oldest_founder_founding_year (Optional[int]): Founding year of the oldest founder.
        oldest_founder_founding_month (Optional[int]): Founding month of the oldest founder.
    
    Returns:
        Dict: The newly created row data if successful, None otherwise.
    
    Raises:
        ValueError: If any of the input values are invalid or in the future.
    """
    if not validate_year(main_founding_year) or not validate_year(oldest_founder_founding_year):
        raise ValueError("Year must be between 1800 and the current year")
    if not validate_month(oldest_founder_founding_month):
        raise ValueError("Month must be between 1 and 12")
    if oldest_founder_founding_year and oldest_founder_founding_month:
        if not validate_date_not_future(oldest_founder_founding_year, oldest_founder_founding_month):
            raise ValueError("Oldest founder founding date cannot be in the future")
    
    data = {"li_id": li_id}
    
    if main_founding_year is not None:
        data["main_founding_year"] = main_founding_year
    if oldest_founder_founding_year is not None:
        data["oldest_founder_founding_year"] = oldest_founder_founding_year
    if oldest_founder_founding_month is not None:
        data["oldest_founder_founding_month"] = oldest_founder_founding_month
    
    try:
        result = supabase.table("founding_dates").insert(data).execute()
        return result.data[0] if result.data else None
    except Exception as e:
        print(f"Error adding new row: {str(e)}")
        return None

def update_row_by_li_id(li_id: str, updates: Dict[str, Union[int, str]]) -> Dict:
    """
    Update an existing row in the founding_dates table.
    
    Args:
        li_id (str): Unique identifier for the row to update.
        updates (Dict[str, Union[int, str]]): Dictionary of column names and their new values.
    
    Returns:
        Dict: The updated row data if successful, None otherwise.
    
    Raises:
        ValueError: If any of the update values are invalid or in the future.
    """
    if "main_founding_year" in updates and not validate_year(updates["main_founding_year"]):
        raise ValueError("Main founding year must be between 1800 and the current year")
    if "oldest_founder_founding_year" in updates and not validate_year(updates["oldest_founder_founding_year"]):
        raise ValueError("Oldest founder founding year must be between 1800 and the current year")
    if "oldest_founder_founding_month" in updates and not validate_month(updates["oldest_founder_founding_month"]):
        raise ValueError("Oldest founder founding month must be between 1 and 12")
    
    if "oldest_founder_founding_year" in updates and "oldest_founder_founding_month" in updates:
        if not validate_date_not_future(updates["oldest_founder_founding_year"], updates["oldest_founder_founding_month"]):
            raise ValueError("Oldest founder founding date cannot be in the future")
    
    # Add updated_at field
    updates["updated_at"] = datetime.utcnow().isoformat()
    
    result = supabase.table("founding_dates").update(updates).eq("li_id", li_id).execute()
    return result.data[0] if result.data else None

def remove_row(li_id: str) -> Dict:
    """
    Remove a row from the founding_dates table.
    
    Args:
        li_id (str): Unique identifier for the row to remove.
    
    Returns:
        Dict: The removed row data if successful, None otherwise.
    """
    result = supabase.table("founding_dates").delete().eq("li_id", li_id).execute()
    return result.data[0] if result.data else None

def get_all_by_value(column: str, value: int) -> List[str]:
    """
    Get all li_ids where a specific column matches a given value.
    
    Args:
        column (str): The column name to search in.
        value (int): The value to search for.
    
    Returns:
        List[str]: A list of li_ids matching the criteria.
    
    Raises:
        ValueError: If the column name is invalid or the value is not a valid year.
    """
    if column not in ["main_founding_year", "oldest_founder_founding_year"]:
        raise ValueError("Invalid column name")
    if not validate_year(value):
        raise ValueError("Year must be between 1800 and the current year")
    
    result = supabase.table("founding_dates").select("li_id").eq(column, value).execute()
    return [row["li_id"] for row in result.data]

def get_all_by_oldest_founder_founding(year: int, month: int) -> List[str]:
    """
    Get all li_ids where the oldest founder's founding year and month match the given values.
    
    Args:
        year (int): The founding year to search for.
        month (int): The founding month to search for.
    
    Returns:
        List[str]: A list of li_ids matching the criteria.
    
    Raises:
        ValueError: If the year or month is invalid or in the future.
    """
    if not validate_year(year):
        raise ValueError("Year must be between 1800 and the current year")
    if not validate_month(month):
        raise ValueError("Month must be between 1 and 12")
    if not validate_date_not_future(year, month):
        raise ValueError("Date cannot be in the future")
    
    result = supabase.table("founding_dates").select("li_id").eq("oldest_founder_founding_year", year).eq("oldest_founder_founding_month", month).execute()
    return [row["li_id"] for row in result.data]

def get_rows_by_creation_date(start_date: datetime, end_date: datetime) -> List[Dict]:
    """
    Get all rows created between two specific dates.
    
    Args:
        start_date (datetime): The start date of the range (inclusive).
        end_date (datetime): The end date of the range (inclusive).
    
    Returns:
        List[Dict]: A list of row data dictionaries matching the criteria.
    """
    result = supabase.table("founding_dates").select("*").gte("created_at", start_date.isoformat()).lte("created_at", end_date.isoformat()).execute()
    return result.data

def update_oldest_founder_founding_if_older(
    li_id: str, 
    new_year: Optional[int] = None, 
    new_month: Optional[int] = None, 
    main_founding_year_value: Optional[int] = None,
    create_if_missing: bool = False
) -> str:
    """
    Update the oldest founder's founding year and month if the new date is older.
    Optionally create a new row if it doesn't exist and update main_founding_year.
    Handles cases where only the year is provided (new_month is None).
    
    Args:
        li_id (str): Unique identifier for the row to update.
        new_year (Optional[int]): The new founding year to compare and potentially update.
        new_month (Optional[int]): The new founding month to compare and potentially update. Can be None.
        main_founding_year_value (Optional[int]): The main founding year to update or use when creating a new row.
        create_if_missing (bool): If True, create a new row when li_id is not found.
    
    Returns:
        str: A status string: "updated", "created", "nothing_changed", or "error"
    """
    try:
        if new_year is not None and not validate_year(new_year):
            raise ValueError("Year must be between 1800 and the current year")
        if new_month is not None and not validate_month(new_month):
            raise ValueError("Month must be between 1 and 12")
        if new_year is not None and new_month is not None and not validate_date_not_future(new_year, new_month):
            raise ValueError("Date cannot be in the future")
        if main_founding_year_value is not None and not validate_year(main_founding_year_value):
            raise ValueError("Main founding year must be between 1800 and the current year")
        
        # Fetch the current row
        result = supabase.table("founding_dates").select("*").eq("li_id", li_id).execute()
        
        if not result.data:
            if create_if_missing:
                # Create a new row
                new_row_data = {
                    "li_id": li_id,
                    "oldest_founder_founding_year": new_year,
                    "oldest_founder_founding_month": new_month,
                    "main_founding_year": main_founding_year_value
                }
                supabase.table("founding_dates").insert(new_row_data).execute()
                return "created"
            else:
                print(f"Row not found for li_id: {li_id}")
                return "error"
        
        current_row = result.data[0]
        current_year = current_row.get("oldest_founder_founding_year")
        current_month = current_row.get("oldest_founder_founding_month")
        
        # Prepare updates
        updates = {}
        
        # Compare and update year and month
        if new_year is not None:
            if current_year is None or new_year < current_year:
                updates["oldest_founder_founding_year"] = new_year
                updates["oldest_founder_founding_month"] = new_month  # This can be None
            elif new_year == current_year:
                if new_month is None and current_month is not None:
                    updates["oldest_founder_founding_month"] = None
                elif new_month is not None and (current_month is None or new_month < current_month):
                    updates["oldest_founder_founding_month"] = new_month
        
        # Update the main_founding_year if provided and different from the current value
        if main_founding_year_value is not None and main_founding_year_value != current_row.get("main_founding_year"):
            updates["main_founding_year"] = main_founding_year_value

        # If there are any updates to make, apply them
        if updates:
            updates["updated_at"] = datetime.now().isoformat()
            clean_date_collected = ""
            if updates["date_collected_date"] == "":
                clean_date_collected = datetime.now().isoformat()
            
            else:
               
                clean_date_collected = datetime.strptime(updates["date_collected"].strip(), "%d-%b-%y").isoformat()
            
            updates["date_collected_date"]=clean_date_collected
            supabase.table("founding_dates").update(updates).eq("li_id", li_id).execute()
            return "updated"
        else:
            return "nothing_changed"
    
    except Exception as e:
        print(f"Error in update_oldest_founder_founding_if_older: {str(e)}")
        return "error"
    




class Raw_Entity(BaseModel):
   
    Headquarters: Optional[str] = ""
    CompType: Optional[str] = ""
    Website: Optional[str] = ""
    numericLink : Optional[str] = ""
    Speciality: Optional[str] = ""
    company_logo_link: Optional[str] = ""
    Founded: Optional[int] = None #>1800
    date_collected: str
    location: Optional[str] = ""
    Overview: Optional[str] = ""
    numberOfEmployees: Optional[int] = None #>0
    Name: str
    verified: Optional[str] = ""
    updated_Link: Optional[str] = ""
    Industry: Optional[str] = ""
    CompanySize: Optional[str] = ""
    

class Clean_Entity(BaseModel):
    
    headquarters: Optional[str] = ""
    company_type: Optional[str] = ""
    website: Optional[str] = ""
    
    speciality: Optional[str] = ""
    
    founded: Optional[int] = None #>1800
    date_collected: str
    location: Optional[str] = ""
    
    number_of_employees: Optional[int] = None #>0
    name: str
    verified: Optional[str] = ""
    
    industry: Optional[str] = ""
    company_size: Optional[str] = ""


def clean_entity(raw_entity: Raw_Entity)->Clean_Entity:
    clean_entity = Clean_Entity(
        
        headquarters=raw_entity.Headquarters,
        company_type=raw_entity.CompType,
        website=raw_entity.Website,
        
        speciality=raw_entity.Speciality,
        
        founded=raw_entity.Founded,
        date_collected=raw_entity.date_collected,
        location=raw_entity.location,
     
        number_of_employees=raw_entity.numberOfEmployees,
        name=raw_entity.Name,
        verified=raw_entity.verified,
        
        industry=raw_entity.Industry,
        company_size=raw_entity.CompanySize,
    )
    return clean_entity

def update_oldest_founder_founding_if_older_add_about(
    li_id: str, 
    raw_entity: Raw_Entity,
    new_year: Optional[int] = None, 
    new_month: Optional[int] = None, 
    main_founding_year_value: Optional[int] = None,
    create_if_missing: bool = False,
    
) -> str:
    """
    Update the oldest founder's founding year and month if the new date is older,
    and add or update the Raw_Entity object in the 'founding_dates' table.
    
    Args:
        li_id (str): Unique identifier for the row to update.
        raw_entity: Raw_Entity
        new_year (Optional[int]): The new founding year to compare and potentially update.
        new_month (Optional[int]): The new founding month to compare and potentially update. Can be None.
        main_founding_year_value (Optional[int]): The main founding year to update or use when creating a new row.
        create_if_missing (bool): If True, create a new row when li_id is not found.
        raw_entity (Raw_Entity): The Raw_Entity object to add or update in the 'founding_dates' table.
    
    Returns:
        str: A status string: "updated", "created", "nothing_changed", or "error"
    """
    try:
        # Validate input
        if new_year is not None and not validate_year(new_year):
            raise ValueError("Year must be between 1800 and the current year")
        if new_month is not None and not validate_month(new_month):
            raise ValueError("Month must be between 1 and 12")
        if new_year is not None and new_month is not None and not validate_date_not_future(new_year, new_month):
            raise ValueError("Date cannot be in the future")
        if main_founding_year_value is not None and not validate_year(main_founding_year_value):
            raise ValueError("Main founding year must be between 1800 and the current year")
        
        # Fetch the current row from entities table
        result = supabase.table("entities").select("*").eq("li_id", li_id).execute()
        
        # Prepare the updates dictionary
        updates = {}
        
        if not result.data:
            if create_if_missing:
                # Prepare data for a new row
                updates = {
                    "li_id": li_id,
                    "oldest_founder_founding_year": new_year,
                    "oldest_founder_founding_month": new_month,
                    "main_founding_year": main_founding_year_value
                }
                status = "created"
            else:
                print(f"Row not found for li_id: {li_id}")
                return "error"
        else:
            current_row = result.data[0]
            current_year = current_row.get("oldest_founder_founding_year")
            current_month = current_row.get("oldest_founder_founding_month")
            
            # Compare and update year and month
            if new_year is not None:
                if current_year is None or new_year < current_year:
                    updates["oldest_founder_founding_year"] = new_year
                    updates["oldest_founder_founding_month"] = new_month  # This can be None
                elif new_year == current_year:
                    if new_month is None and current_month is not None:
                        updates["oldest_founder_founding_month"] = None
                    elif new_month is not None and (current_month is None or new_month < current_month):
                        updates["oldest_founder_founding_month"] = new_month
            
            # Update the main_founding_year if provided and different from the current value
            if main_founding_year_value is not None and main_founding_year_value != current_row.get("main_founding_year"):
                updates["main_founding_year"] = main_founding_year_value

            status = "updated" if updates else "nothing_changed"
        
        # Add the Clean_Entity data to the updates
        clean_entity_data = clean_entity(raw_entity).dict()
        updates.update(clean_entity_data)
        
        # Add or update the timestamp
        updates["updated_at"] = datetime.now().isoformat()
        
        clean_date_collected = ""
        
        if updates["date_collected"] =="":
            clean_date_collected = datetime.now().isoformat()
            
        else:
            clean_date_collected = datetime.strptime(updates["date_collected"].strip(), "%d-%b-%y").isoformat()
        
        
        updates["date_collected_date"]=clean_date_collected
        
        # Perform the update or insert
        if status == "created":
            supabase.table("entities").insert(updates).execute()
        else:
            supabase.table("entities").update(updates).eq("li_id", li_id).execute()
        
        return status
    
    except Exception as e:
        print(f"Error in update_oldest_founder_founding_if_older_add_about: {str(e)}")
        if 'JWT expired' in str(e):
            supabase_client = connect_to_supabase()
            if isinstance(supabase_client, int) and supabase_client < 0:
                print("Failed to connect to Supabase")
                return
            else:
                print("connected again meche l7al")
        return "error"

    


             


# Example 1: Get all rows where the oldest founder's founding year is 1990 and month is 6
def example_get_all_by_oldest_founder_founding():
    year = 1990
    month = 6
    results = get_all_by_oldest_founder_founding(year, month)
    print(f"Companies founded by oldest founder in {month}/{year}:")
    for li_id in results:
        print(f"- {li_id}")

# Example 2: Get all rows created between two specific dates
def example_get_rows_by_creation_date():
    start_date = datetime(2023, 1, 1)  # January 1, 2023
    end_date = datetime(2023, 12, 31)  # December 31, 2023
    results = get_rows_by_creation_date(start_date, end_date)
    print(f"Rows created between {start_date.date()} and {end_date.date()}:")
    for row in results:
        print(f"- LI ID: {row['li_id']}, Created at: {row['created_at']}, Updated at: {row['updated_at']}")

# Example 3: Update oldest founder founding date if newer
def example_update_oldest_founder_founding_if_older():
    li_id = "example_company_3"
    new_year = 2024
    new_month = 3
    main_founding_year = 1990
    create_if_missing = True
    
    result = update_oldest_founder_founding_if_older(
        li_id, 
        new_year, 
        new_month, 
        main_founding_year_value=main_founding_year,
        create_if_missing=create_if_missing
    )
    print(result)
    
def example_update_founding_add_about():
    """
    Example function demonstrating the use of update_oldest_founder_founding_if_older_add_about
    with Hugging Face company data.
    """
    # Hugging Face company data
    li_id = "huggingface_2"
    new_year = 2016  # Founded year from the provided data
    new_month = None  # We don't have the specific month, so we'll use None
    main_founding_year = 2016
    create_if_missing = True

    # Create a Raw_Entity object with Hugging Face data
    raw_entity = Raw_Entity(
        Name="Hugging Face 2",
        Founded=2016,
        date_collected="09-Oct-24",
        Headquarters="Amsterdam, Noord-Holland",
        CompType="",
        Website="huggingface.co",
        numericLink="https://www.linkedin.com/company/11193683",
        Speciality="machine learning, natural language processing, and deep learning",
        company_logo_link="https://media.licdn.com/dms/image/v2/C4D0BAQFzIxlpQ0lAdA/company-logo_200_200/company-logo_200_200/0/1630556211624/huggingface_logo?e=1736380800&v=beta&t=l59QwRoXZxcDLcPwCjwhWpFXPLfxfYrCHk1NZc0YXjc",
        location="",
        Overview="The AI community building the future.",
        numberOfEmployees=444,
        verified="September 12, 2023",
        updated_Link="https://www.linkedin.com/company/huggingface/",
        Industry="Software Development",
        CompanySize="51-200 employees"
    )

    # Call the function
    result = update_oldest_founder_founding_if_older_add_about(
        li_id,
        raw_entity,
        new_year,
        new_month,
        main_founding_year_value=main_founding_year,
        create_if_missing=create_if_missing
        )

    # Print the result
    print(f"Operation result: {result}")

    # Fetch and display the updated record
    updated_record = supabase.table("entities").select("*").eq("li_id", li_id).execute()
    
    if updated_record.data:
        print("\nUpdated record:")
        for key, value in updated_record.data[0].items():
            print(f"{key}: {value}")
    else:
        print("No record found after update.")
        
        


"""

"""

def search_entities_multi(**kwargs: Any) -> List[Dict[str, Any]]:
    """
    Search for rows in the entities table that match the given filters using Supabase filter method.
    All filters use the 'in' operator with a list format.
    
    Args:
        **kwargs: Arbitrary keyword arguments representing column names and their values to filter by.
    
    Returns:
        List[Dict[str, Any]]: A list of entities that match the filters, or an empty list if no matches are found.
    """
    try:
        query = supabase.table("entities").select("*")
        
        for column, value in kwargs.items():
            if isinstance(value, (str, int, float)):
                # Convert single values to a list with one item
                value = [value]
            
            if isinstance(value, list):
                if all(isinstance(item, (int, float)) for item in value):
                    # For numeric lists
                    in_values = ",".join(map(str, value))
                elif all(isinstance(item, str) for item in value):
                    # For string lists
                    in_values = ",".join(f'"{item}"' for item in value)
                else:
                    print(f"Warning: Mixed types in list for column '{column}'. Skipping this filter.")
                    continue
                
                query = query.filter(column, "in", f"({in_values})")
            elif value is None:
                # Handle None values
                query = query.filter(column, "is", "null")
            else:
                print(f"Warning: Unsupported type for column '{column}'. Skipping this filter.")
        
        # Print the SQL that would be generated
        print("SQL that would be generated:")
        #print(query)
        
        # Execute the query
        response = query.execute()
        
        return response.data
    
    except Exception as e:
        print(f"Error in search_entities_multi: {str(e)}")
        return []



def example_search_entities():
    

    
    result = search_entities_multi(
        #name="Hugging Face 2",
        #founded=[2015, 2016, 2017],  # Using a list for founded year
        date_collected="08-Oct-24",
        #headquarters="Amsterdam, Noord-Holland",
        #comp_type="",  # Empty string to match empty values
        website="huggingface.co"
        
        #speciality="machine learning, natural language processing, and deep learning",
        
        #location="",  # Empty string to match empty values
        
        #number_of_employees=444,
        #verified="September 12, 2023",
        
        #industry="Software Development",
        #company_size="51-200 employees"
    )
    result = search_entities_case_insensitive(
        #name="Hugging Face",
        #founded=[2015, 2016, 2017],
        #industry="Software Development",
        website="huggingface.co"
    )
    
    print("Companies matching all criteria:")
    print(result)

    # If you want to see more details about the matched companies:
    if result:
        details = supabase.table("entities").select("*").in_("li_id", result).execute()
        for company in details.data:
            print("\nCompany Details:")
            for key, value in company.items():
                print(f"{key}: {value}")
    else:
        print("No companies found matching the criteria.")
        
def search_entities_case_insensitive(**kwargs: Any) -> List[Dict[str, Any]]:
    """
    Search for rows in the entities table that match the given filters using Supabase filter method.
    String searches are case-insensitive. 
    
    Args:
        **kwargs: Arbitrary keyword arguments representing column names and their values to filter by.
    
    Returns:
        List[Dict[str, Any]]: A list of entities that match the filters, or an empty list if no matches are found.
    """
    try:
        query = supabase.table("entities").select("li_id")
        
        for column, value in kwargs.items():
            print("column:",column)
            print("value:",value)
            if isinstance(value, (str, int, float)):
                # Convert single values to a list with one item
                value = [value]
                print("** value:", value)
            if isinstance(value, list):
                if all(isinstance(item, str) for item in value):
                    print("all(isinstance(item, str) for item in value)")
                    # For string lists, use 'ilike' for case-insensitive search
                    for item in value:
                        
                        query = query.ilike(column, f"*{item}*")
                elif all(isinstance(item, (int, float)) for item in value):
                    # For numeric lists, use 'in' operator
                    in_values = ",".join(map(str, value))
                    query = query.filter(column, "in", f"({in_values})")
                else:
                    print(f"Warning: Mixed types in list for column '{column}'. Skipping this filter.")
                    continue
            elif value is None:
                # Handle None values
                query = query.filter(column, "is", "null")
            else:
                print(f"Warning: Unsupported type for column '{column}'. Skipping this filter.")
        
        # Print the SQL that would be generated
        print("SQL that would be generated:")
        
        
        # Execute the query
        response = query.execute()
        
        return response.data
    
    except Exception as e:
        print(f"Error in search_entities_case_insensitive: {str(e)}")
        return []
    

def convert_and_filter(experiences):
    valid_dates = []
    invalid_dates = []
    for exp in experiences:
        if not exp.get("period_start") or not exp.get("period_end"):
            continue  # Skip experiences without period_start or period_end
        short_start_year=False
        try:
            # Handle empty strings
            if not exp["period_start"].strip():
                exp["period_start"] = "Jan 1970"  # Use a default date or skip
            if not exp["period_end"].strip():
                exp["period_end"] = "Dec 9999"  # Use a default date or skip

            # Check if period_start is in the format 'YYYY'
            if len(exp["period_start"].strip()) == 4:
                short_start_year=exp["period_start"].strip()
                exp["period_start"] = datetime.strptime(exp["period_start"].strip() + " 01", "%Y %m")
            else:
                exp["period_start"] = datetime.strptime(exp["period_start"].strip(), "%b %Y")
            
            # Check if period_end is in the format 'YYYY'
            if exp["period_end"].strip().lower() == "present":
                exp["period_end"] = "Present"
            elif len(exp["period_end"].strip()) == 4:
                if short_start_year!=False:
                    if short_start_year == exp["period_end"].strip():
                        exp["period_end"] = datetime.strptime(exp["period_end"].strip() + " 06", "%Y %m")
                    else:
                        adjusted_year=str(int(exp["period_end"].strip())-1)
                        exp["period_end"] = datetime.strptime(adjusted_year + " 12", "%Y %m")
                else:
                    exp["period_end"] = exp["period_start"]
            else:
                exp["period_end"] = datetime.strptime(exp["period_end"].strip(), "%b %Y")
                
            valid_dates.append(exp)
        except ValueError as e:
            print(f"Date parsing error: {e}")
            invalid_dates.append(exp)
    
    return valid_dates, invalid_dates

def determine_entry_type(prev_exp, curr_exp):
    """Determine the type of entry for the current experience."""
    
    if prev_exp['company_name'] != curr_exp['company_name']:
        return "in"
    else:
        
        if prev_exp['period_end'] == "Present" or curr_exp['period_start'] < prev_exp['period_end']:
          
            return "additional_title"
        return "promotion"
    
        """
        elif curr_exp['period_start'] - prev_exp['period_end'] < timedelta(days=60):
            return "promotion"
        
       
        else:
            return "in"
        
        if prev_exp['title'] in curr_exp['title']:
            return "additional_title"
        if prev_exp['title'] != curr_exp['title']:
            return "promotion"
        """
        
def determine_exit_type(prev_exp, curr_exp):
    """Determine the type of exit for the previous experience."""
    prev_end = prev_exp['period_end']
    curr_start = curr_exp['period_start']
    
    #if prev_end == "Present":
    #    return "dropped_title"
    if isinstance(prev_end, str):
        prev_end = datetime.strptime(prev_end.strip(), "%b %Y")
    if isinstance(curr_start, str):
        curr_start = datetime.strptime(curr_start.strip(), "%b %Y")
    #print("@@@ prev_exp['company_name']:", prev_exp['company_name'])
    
    
    if prev_exp['company_name'] != curr_exp['company_name']:
        return "out"
    else:
        return "dropped_title"
        
        """
        if prev_end < curr_start:
            return "dropped_title"
        else:
            return "promotion"
        """

def increment_month_year(month, year):
    """Increment the month and adjust the year if necessary."""
    if month == 12:
        return 1, year + 1
    else:
        return month + 1, year

import glob    

def load_json_files(directory_path):
    """Load all JSON files from the specified directory."""
    file_paths = glob.glob(f'{directory_path}/*.json')
    profiles = []
    for file_path in file_paths:
        with open(file_path, 'r') as file:
            profile = json.load(file)
            profiles.append(profile)
    return profiles


def extract_id(url):
    # Remove trailing slash if present
    clean_url = url.rstrip('/')
    # Split by '/' and get the last part
    return clean_url.rsplit('/', 1)[-1]

def extract_person_linkedin_handle(url):
    """Extract the LinkedIn handle of the person from the URL."""
    if url and 'linkedin.com' in url:
        if '/about/' in url:
            url = url.split("/about")[0]
            
        url_to_return = url.split('/in/')[1]
        if '/' in url_to_return:
            url = url_to_return.strip("/")
        else:
            url = url_to_return
        return url
    return None

def extract_company_linkedin_id(url):
    """Extract the LinkedIn company ID from the URL."""
    if url and 'linkedin.com' in url:
        if 'authwall?' in url:
            url_to_return = ""
        if '/company/' in url:
            url_to_return = url.split('/company/')[1]
        elif '/school/' in url:
            url_to_return = url.split('/school/')[1]
        elif '/showcase/' in url:
            url_to_return = url.split('/showcase/')[1]
        if '/' in url_to_return:
            url = url_to_return.replace("/", "")
        else:
            url = url_to_return
        return url
    return None



    
def process_experience(profile):
    """Process experiences for a given profile."""
    data = []
    person_linkedin_handle = extract_person_linkedin_handle(profile['general'].get('linkedin_url'))
    if person_linkedin_handle == None:
        return -1
    experiences = profile.get('experience', [])
    experiences, _ = convert_and_filter(experiences)
    
    for i, exp in enumerate(experiences):
        if exp.get('period_end') in ["present","Present"]:
            experiences[i]["period_end"]=datetime.strptime(  "9999 12", "%Y %m")
    try:
        experiences = sorted(experiences, key=lambda x: x.get("period_end"))
        for i, exp in enumerate(experiences):
            if exp.get('period_end') == datetime.strptime(  "9999 12", "%Y %m"):
                experiences[i]["period_end"]="Present"
        
    except TypeError as e:
        print("%%%%%%%% TypeError")
        print(experiences)
        raise Exception("TypeError")
        
    
    if 'hashed_name' not in profile.keys():
        profile['hashed_name']=''
        
    for i, exp in enumerate(experiences):
        company_linkedin_id = extract_company_linkedin_id(exp.get('company_linkedin_url'))
        #print(" ** company_linkedin_id:", company_linkedin_id)
        #print("period_start:",exp.get('period_start'))
        #print("period_end:",exp.get('period_end'))
        
        if not exp.get('period_start') or not exp.get('period_end'):
            continue
        start_month, start_year = exp.get('period_start').month, exp.get('period_start').year
        if exp.get('period_end') in ["9999", "Present"]:
            
            end_month, end_year = None, None
        else:
            end_month, end_year = exp.get('period_end').month, exp.get('period_end').year
        
        current=False
        if exp.get('period_end') == 'Present':
            current=True
        in_row = {
            'company_name': exp.get("company_name"),
            'linkedin_handle_of_company': company_linkedin_id,
            'linkedin_handle_of_person': person_linkedin_handle,
            'type': "in",
            'title': exp.get('title'),
            'month': start_month,
            'year': start_year,
            'hashed_name':profile['hashed_name'],
            'company_experience_location': exp.get('company_experience_location'),
            'current': current
        }
        if i > 0:
            in_row['type'] = determine_entry_type(experiences[i - 1], exp)
        data.append(in_row)

        
        if exp.get('period_end') != "Present":
            out_month, out_year = increment_month_year(end_month, end_year)
            out_row = {
                'company_name': exp.get("company_name"),
                'linkedin_handle_of_company': company_linkedin_id,
                'linkedin_handle_of_person': person_linkedin_handle,
                'type': "out",
                'title': exp.get('title'),
                'month': out_month,
                'year': out_year,
                'hashed_name':profile['hashed_name'],
                'company_experience_location': exp.get('company_experience_location'),
                'current': current
                
            }
            if i < len(experiences) - 1:
                #ex_type = determine_exit_type(exp, experiences[i + 1])
                out_row['type'] = determine_exit_type(exp, experiences[i + 1])
                data.append(out_row)
        
            
    return data
        


class Person(BaseModel):
    li_id: str 
    name: Optional[str] = ""
    header: Optional[str] = ""
    location: Optional[str] = ""
    date_collected: Optional[str] = ""
    date_collected_date: Optional[datetime]=None

def upsert_person(person: Person):
    # Prepare the data for upsert
    person_data = person.dict()
    
    person_data["date_collected_date"]=datetime.strptime(person_data["date_collected"].strip(), "%d-%b-%y").isoformat()
    # Check if a person with this li_id already exists
    result = supabase.table('people').select('id').eq('li_id', person.li_id).execute()

    if result.data:
        # If the person exists, update the existing record
        existing_id = result.data[0]['id']
        result = supabase.table('people').update(person_data).eq('id', existing_id).execute()
        print(f"Updated person with li_id: {person.li_id}")
    else:
        # If the person doesn't exist, insert a new record
        result = supabase.table('people').insert(person_data).execute()
        print(f"Inserted new person with li_id: {person.li_id}")

    # Check if the operation was successful
    if result.data:
        return result.data[0]
    else:
        print(f"Failed to upsert person with li_id: {person.li_id}")
        return -1
    
 
def update_titles(new_titles: List[Dict], linkedin_handle: str):
    """
    Updates the titles table with new entries for a specific LinkedIn handle while preserving existing ones.
    
    Args:
        new_titles (List[Dict]): List of title dictionaries to be added
        linkedin_handle (str): LinkedIn handle to process titles for
        
    Returns:
        int: Number of titles added, 0 if none added, -1 if error, -2 if exception
    """
    
    titles_added = 0
    
    try:
        
        #remove all titles for person with current == True
        try:
            
            
            # Delete records matching criteria
            result = supabase.table('titles') \
                .delete() \
                .eq('linkedin_handle_of_person', linkedin_handle) \
                .eq('current', True) \
                .execute()
                
            #print(f"Successfully deleted records for handle: {linkedin_handle}")
            
            
        except Exception as e:
            print(f"Error deleting records: {str(e)}")
            return -3
            
        
        # Get existing titles for this LinkedIn handle
        existing_titles = supabase.table('titles') \
            .select('*') \
            .eq('linkedin_handle_of_person', linkedin_handle) \
            .execute()
        
        #print(f"Found {len(existing_titles.data)} existing titles")
        
   
        
        # Handle the empty case explicitly
        if not existing_titles.data:
            existing_set = set()
        else:
            # Convert existing titles to a set of tuples for easy comparison
            # Only include fields that we want to check for duplicates
            existing_set = {
                (
                    title['company_name'],
                    title['type'],
                    title['title'],
                    title['month'],
                    title['year'],
                    title['hashed_name'],
                    title['current'],
                    title['company_experience_location'],
                    title['linkedin_handle_of_company']
                )
                for title in existing_titles.data if title['current']!=True
            }
            
       # print(f"Existing set size: {len(existing_set)}")
        #if existing_set:
        #    print("Sample existing tuple:", next(iter(existing_set)))
        
        #print("existing_set:")
        #print(existing_set)
        # Current date for date_collected field
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        all_new_titles_set = {
            (
                title['company_name'],
                title['type'],
                title['title'],
                title['month'],
                title['year'],
                title['hashed_name'],
                title['current'],
                title['company_experience_location'],
                title['linkedin_handle_of_company']
            )
            for title in new_titles
        }
        #print("----- *all_new_titles_set*")
        
        #check the history before current hasn't changed.
        for title in existing_titles.data:
            title_tuple = (
                title['company_name'],
                title['type'],
                title['title'],
                title['month'],
                title['year'],
                title['hashed_name'],
                title['current'],
                title['company_experience_location'],
                title['linkedin_handle_of_company']
            )
            #print("title_tuple:")
            #print(title_tuple)
            if title_tuple not in all_new_titles_set:
                #History is broken. delete all records for user in titles tables and upload all the new titles.
                #remove all titles for person with current == True
                try:
                    
                    
                    # Delete records matching criteria
                    result = supabase.table('titles') \
                        .delete() \
                        .eq('linkedin_handle_of_person', linkedin_handle) \
                        .execute()
                        
                    #print(f"Successfully deleted All records for handle (before adding all from new set): {linkedin_handle}")
                    existing_set = set()
                    break
                    
                    
                except Exception as e:
                    print(f"Error deleting records: {str(e)}")
                    return -4                
                    
                
        
        # Process new titles
        titles_to_add = []
        for title in new_titles:
            # Create tuple for comparison using the same fields
            title_tuple = (
                title['company_name'],
                title['type'],
                title['title'],
                title['month'],
                title['year'],
                title['hashed_name'],
                title['current'],
                title['company_experience_location'],
                title['linkedin_handle_of_company']
            )
            
            #print("title_tuple:", title_tuple)
            if title_tuple not in existing_set:
                #print(f"New title to add: {title['company_name']} - {title['title']}")
                #if 'date_collected' not in title:
                #    title['date_collected'] = current_date
                titles_to_add.append(title)
            #else:
                #print(f"Skipping existing title: {title['company_name']} - {title['title']}")
        
        # If we have new titles to add, insert them after removing all rows for person with current = True
        if titles_to_add:
            
            
            
            
            #print(f"Adding {len(titles_to_add)} new titles")
            result = supabase.table('titles') \
                .insert(titles_to_add) \
                .execute()
            
            titles_added = len(titles_to_add)
            
            if hasattr(result, 'error') and result.error:
                print("error: hasattr(result, 'error') and result.error")
                return -1
            return titles_added
        else:
            #print("No new titles to add")
            return 0
        
    except Exception as e:
        print(f"Error processing titles: {str(e)}")
        return -2




# Helper function to print the actual data in the database
def print_db_contents(linkedin_handle: str):
    try:
        result = supabase.table('titles') \
            .select('*') \
            .eq('linkedin_handle_of_person', linkedin_handle) \
            .execute()
        
        print("\nCurrent database contents:")
        for title in result.data:
            print(f"{title['company_name']} - {title['title']} ({title['type']}) - {title['year']}")
    except Exception as e:
        print(f"Error fetching database contents: {str(e)}")    

def upsert_education(li_id: str, new_educations: List[Dict]):
    """
    Updates education records for a given LinkedIn ID. Adds new records and maintains existing ones.
    
    Args:
        li_id (str): LinkedIn ID of the person
        new_educations (List[Dict]): List of education records to upsert
        
    Returns:
        int: Number of records added, 0 if none added, -1 if error, -2 if exception
    """
    try:
        # Get existing education records for this li_id
        existing_records = supabase.table('education') \
            .select('*') \
            .eq('li_id', li_id) \
            .execute()
        
        #print(f"Found {len(existing_records.data)} existing education records")
        
        # Create a set of tuples for existing records
        if not existing_records.data:
            existing_set = set()
        else:
            existing_set = set()
            for record in existing_records.data:
                # Convert years to int, handling possible None/empty values
                start_year = int(record['education_start_year']) if record['education_start_year'] else 0
                end_year = int(record['education_end_year']) if record['education_end_year'] else 0
                
                existing_set.add((
                    str(record['institution_name'] or ''),
                    str(record['degree'] or ''),
                    str(record['major'] or ''),
                    start_year,
                    end_year
                ))
            
        #print(f"Existing set size: {len(existing_set)}")
        
        # Process new education records
        records_to_add = []
        for edu in new_educations:
            # Convert years to int, handling possible None/empty values
            try:
                start_year = int(edu['education_start_year']) if edu['education_start_year'] else 0
            except (ValueError, TypeError):
                start_year = 0
                
            try:
                end_year = int(edu['education_end_year']) if edu['education_end_year'] else 0
            except (ValueError, TypeError):
                end_year = 0
            
            # Create comparison tuple
            edu_tuple = (
                str(edu['institution_name'] or ''),
                str(edu['degree'] or ''),
                str(edu['major'] or ''),
                start_year,
                end_year
            )
            
            # Prepare the record for insertion
            education_record = {
                'li_id': li_id,
                'institution_name': edu['institution_name'],
                'degree': edu['degree'],
                'major': edu['major'],
                'education_start_year': start_year if start_year != 0 else None,
                'education_end_year': end_year if end_year != 0 else None,
                'institution_url': edu.get('institution_url', ''),
                "institution_li_id": extract_id(edu.get('institution_linkedin_url', ''))
            }
            
            if edu_tuple not in existing_set:
                #print(f"New education to add: {education_record['institution_name']} - {education_record['degree']}")
                records_to_add.append(education_record)
            else:
                pass
                #print(f"Skipping existing education: {education_record['institution_name']} - {education_record['degree']}")
        
        # Insert new records if any
        if records_to_add:
            #print(f"Adding {len(records_to_add)} new education records")
            result = supabase.table('education') \
                .insert(records_to_add) \
                .execute()
            
            if hasattr(result, 'error') and result.error:
                print(f"Error inserting records: {result.error}")
                return -1
            
            return len(records_to_add)
        else:
            #print("No new education records to add")
            return 0
            
    except Exception as e:
        print(f"Error processing education records: {str(e)}")
        print("Error details:", e._class.name_)
        import traceback
        print(traceback.format_exc())
        return -2




#JOY
#to call when collecting ppl and sales ppl
def update_person(
    li_id: str, 
    ppl_profile: dict,
    
) -> int:
    """


    Parameters
    ----------
    li_id : str
        DESCRIPTION.
    ppl_profile : dict
        DESCRIPTION.

    Returns
    -------
    int
        0 for success. negative numbers for failures

    """
    
    #update gneral
    
    prof=ppl_profile.copy()
    person = Person(li_id=li_id, name=prof["general"]["name"], header=prof["general"]["header"], location=prof["general"]["location"], date_collected=prof["ppl"]["date_collected"])
    ret = upsert_person(person)
    if ret == -1:
        print("error in upsert_person. Will not proceed with operation. Failure.")
        return -1
    
    #update experience
    processed_exp = process_experience(prof)
    if processed_exp == -1:
        print("error in process_experience. Will not proceed with operation. Failure.")
        return -2
   
    titles_added=update_titles(processed_exp, li_id)
    if titles_added<0:
        print("error in update_titles. Will not proceed with operation. Failure.")
        return -3    
    
    #update education
    ret=upsert_education(li_id, prof["education"])
    if ret<0:
        print("error in upsert_education. Will not proceed with operation. Failure.")
        return -4    
    
    #success
    return 0


def update_person_example():

    profiles = load_json_files("temp_profiles")
    
    for prof in profiles:
        li_id=extract_person_linkedin_handle(prof["general"]["linkedin_url"])
        
        if li_id==-1:
            print("error")
        ret = update_person(li_id, prof)
        #just one
        
    if ret == 0:
        print("!!! success")
    else:
        print("failure")

    
#JOY
#To call ret=run_supabase_function('get_numeric_handles') from loop or cloud function (once every 30 minutes using scheduler)
def run_supabase_function(function_name):   
    try:
        return supabase.rpc(function_name).execute().data
    except Exception as e:
        print(f"Error executing function {function_name}: {str(e)}")
        return -1
#ret=run_supabase_function('get_numeric_handles')

#JOY
#to call in worker once the alhpa is retrieved.
def replace_linkedin_handle(old_value, new_value):
    """
    Replace a specific linkedin_handle_of_company value with a new value
    
    Args:
      
        old_value (str): The current handle value to replace
        new_value (str): The new handle value to set
    
    Returns:
        The result of the update
    """
    try:
        result = (supabase.table('titles')
                 .update({'linkedin_handle_of_company': new_value})
                 .eq('linkedin_handle_of_company', old_value)
                 .execute())
        return result
    except Exception as e:
        print(f"Error executing update: {str(e)}")
        return -1
#ret=replace_linkedin_handle("71175633","ABCABCABC")
#if ret!=-1:
#    print(len(ret.data))


#Joy. add this for loop processing the html of advanced search results. to add the sales id and li_id to supabase
def add_sales_id_people(li_id, sales_link):
    try:
        if sales_link[-1] == "/":
            sales_link = sales_link[:-1]
            
        sales_id = sales_link.rsplit("/")[-1]
        
        # Check if record exists
        response = supabase.table('people_sales').select("*").eq('li_id', li_id).execute()
        
        if not response.data:
            # Insert new record if doesn't exist
            supabase.table('people_sales').insert({
                'li_id': li_id,
                'sales_id': sales_id
            }).execute()
            
        return 0
    
    except Exception as e:
        print(f"Error executing add_sales_id_people: {str(e)}")
        if 'JWT expired' in str(e):
            supabase_client = connect_to_supabase()
            if isinstance(supabase_client, int) and supabase_client < 0:
                print("Failed to connect to Supabase")
                return
        return -1
    
    
# ret=add_sales_id_people("someli_id_1","https://www.linkedin.com/sales/lead/ACwAAAIgV0QBrPkQUIZi82pxooz8MTJdSSVY_a4,NAME_SEARCH,EHF-")


"""
Ran this in sql editor in supabase
-- Enable RLS on the table (it's likely already enabled, but just to be sure)
ALTER TABLE public.founding_dates ENABLE ROW LEVEL SECURITY;

-- Policy for inserting rows (adjust as needed)
CREATE POLICY "Enable insert for authenticated users only" ON public.founding_dates FOR INSERT TO authenticated WITH CHECK (true);

-- Policy for selecting rows (adjust as needed)
CREATE POLICY "Enable select for authenticated users only" ON public.founding_dates FOR SELECT TO authenticated USING (true);

-- Policy for updating rows (adjust as needed)
CREATE POLICY "Enable update for authenticated users only" ON public.founding_dates FOR UPDATE TO authenticated USING (true);

-- Policy for deleting rows (adjust as needed)
CREATE POLICY "Enable delete for authenticated users only" ON public.founding_dates FOR DELETE TO authenticated USING (true);


"""
#examples:
"""

add_new_row("kd1213sdd",2020, 2019,11)
add_new_row("ksajas32",oldest_founder_founding_year= 2019)
add_new_row("some121",oldest_founder_founding_year= 2019, oldest_founder_founding_month=11)
add_new_row("anothe12422",oldest_founder_founding_year= 2020, main_founding_year=2020)
result=get_all_by_value("oldest_founder_founding_year",2020)
result=get_all_by_value("main_founding_year",2020)
result=update_row_by_li_id("ksajas32",updates={"oldest_founder_founding_month":12})

result=get_all_by_oldest_founder_founding(2019,12)

start_date = datetime(2024, 8, 29)  # 
end_date = datetime(2024, 8, 30)  # D
results = get_rows_by_creation_date(start_date, end_date)
print(f"Rows created between {start_date.date()} and {end_date.date()}:")
for row in results:
    print(f"- LI ID: {row['li_id']}, Created at: {row['created_at']}")

result=get_all_by_oldest_founder_founding(2019,12)

li_id = "anothe12422"
new_year = 2020
new_month = 1
result = update_oldest_founder_founding_if_newer(li_id, new_year, new_month)
if result:
    print(f"Updated record for {li_id}:")
    print(f"New oldest founder founding date: {result['oldest_founder_founding_month']}/{result['oldest_founder_founding_year']}")
else:
    print(f"No update needed for {li_id}")

"""

class CustomArgument(BaseModel):
    sign: Literal["eq","neq","gt","gte","lt","lte","like","ilike","is","is_not","in","not_in","text_search"]
    value: Any
    config: Optional[Dict[str, Any]] = None  # For additional text search configuration



def search_across_tables_people(
    people_filters: Optional[List[Dict[str, Any]]] = None,
    education_filters: Optional[List[Dict[str, Any]]] = None,
    titles_filters: Optional[List[Dict[str, Any]]] = None,
    range_from: Optional[int] = 0,
    range_to: Optional[int] = None
) -> List[str]:
    """
    Search across people, education, and titles tables with custom filtering.
    Returns a list of matching li_ids with pagination support.
    Maximum window size is 1000 records (range_to must be <= range_from + 999).
    Each filter type can receive multiple filter sets that are combined with AND logic.
    
    Args:
        people_filters: List of filter dictionaries for people table
        education_filters: List of filter dictionaries for education table
        titles_filters: List of filter dictionaries for titles table
        range_from: Starting index for pagination (default: 0)
        range_to: Ending index for pagination (must be <= range_from + 999)
        
    Returns:
        List[str]: List of matching li_ids
    """
    if not supabase:
        return -3

    # Require at least one filter
    if not any([people_filters, education_filters, titles_filters]):
        return -2

    # Validate range parameters
    if range_from < 0:
        range_from = 0
    
    max_range_to = range_from + 999
    if range_to is None:
        range_to = max_range_to
    elif range_to > max_range_to:
        range_to = max_range_to
    elif range_to < range_from:
        return -2

    try:
        matching_li_ids = None

        # Query education table if filters exist
        if education_filters:
            edu_li_ids = None
            for filter_set in education_filters:
                edu_query = supabase.table("education").select("li_id", count="exact")
                edu_query = apply_filters(edu_query, filter_set)
                edu_response = edu_query.execute()
                current_edu_li_ids = {row['li_id'] for row in edu_response.data if row.get('li_id')}
                
                if edu_li_ids is None:
                    edu_li_ids = current_edu_li_ids
                else:
                    edu_li_ids &= current_edu_li_ids
                    
                if not edu_li_ids:  # No matches for this AND condition
                    return []
            
            if matching_li_ids is None:
                matching_li_ids = edu_li_ids
            else:
                matching_li_ids &= edu_li_ids

            if not matching_li_ids:  # No matches, can exit early
                return []

        # Query titles table if filters exist
        if titles_filters:
            titles_li_ids = None
            for filter_set in titles_filters:
                titles_query = supabase.table("titles").select("linkedin_handle_of_person", count="exact")
                titles_query = apply_filters(titles_query, filter_set)
                titles_response = titles_query.execute()
                current_titles_li_ids = {row['linkedin_handle_of_person'] for row in titles_response.data if row.get('linkedin_handle_of_person')}
                
                if titles_li_ids is None:
                    titles_li_ids = current_titles_li_ids
                else:
                    titles_li_ids &= current_titles_li_ids
                    
                if not titles_li_ids:  # No matches for this AND condition
                    return []
            
            if matching_li_ids is None:
                matching_li_ids = titles_li_ids
            else:
                matching_li_ids &= titles_li_ids

            if not matching_li_ids:  # No matches, can exit early
                return []

        # Query people table if filters exist
        if people_filters:
            people_li_ids = None
            for filter_set in people_filters:
                people_query = supabase.table("people").select("li_id", count="exact")
                people_query = apply_filters(people_query, filter_set)
                people_response = people_query.execute()
                current_people_li_ids = {row['li_id'] for row in people_response.data if row.get('li_id')}
                
                if people_li_ids is None:
                    people_li_ids = current_people_li_ids
                else:
                    people_li_ids &= current_people_li_ids
                    
                if not people_li_ids:  # No matches for this AND condition
                    return []
            
            if matching_li_ids is None:
                matching_li_ids = people_li_ids
            else:
                matching_li_ids &= people_li_ids

            if not matching_li_ids:  # No matches, can exit early
                return []

        # Get final ordered results with range
        if matching_li_ids:
            final_query = supabase.table("people").select("li_id")
            final_query = final_query.filter("li_id", "in", f'({",".join(f"{id}" for id in matching_li_ids)})')
            final_query = final_query.order('id', desc=True)
            final_query = final_query.range(range_from, range_to)
            final_response = final_query.execute()
            
            return [row['li_id'] for row in final_response.data if row.get('li_id')]

        return []

    except Exception as e:
        print(f"Error in search_across_tables_people: {str(e)}")
        return -1

def apply_filters(query, filters: Dict[str, Any]) -> Any:
    """
    Apply filters to a query based on the provided filter dictionary.
    
    Args:
        query: The base Supabase query
        filters: Dictionary of filters to apply
        
    Returns:
        Modified query with filters applied
    """
    for column, value in filters.items():
        if isinstance(value, CustomArgument):
            if value.sign == "text_search":
                if isinstance(value.value, str):
                    query = query.text_search(column, value.value)
                else:
                    print(f"Warning: Text search value must be a string for column '{column}'. Skipping this filter.")
            
            elif value.sign == "in" and isinstance(value.value, list):
                if all(isinstance(item, (int, float)) for item in value.value):
                    # For numeric lists
                    in_values = ",".join(map(str, value.value))
                    query = query.filter(column, "in", f"({in_values})")
                elif all(isinstance(item, str) for item in value.value):
                    # For string lists
                    in_values = ",".join(f'"{item}"' for item in value.value)
                    query = query.filter(column, "in", f"({in_values})")
                else:
                    print(f"Warning: Mixed types in list for column '{column}'. Skipping this filter.")
            else:
                query = query.filter(column, value.sign, value.value)
            
        elif isinstance(value, (str, int, float)):
            # Convert single values to equality filter
            query = query.filter(column, "eq", value)
            
        elif isinstance(value, list):
            if not value:
                continue
                
            if all(isinstance(item, (int, float)) for item in value):
                # For numeric lists
                in_values = ",".join(map(str, value))
                query = query.filter(column, "in", f"({in_values})")
                
            elif all(isinstance(item, str) for item in value):
                # For string lists
                in_values = ",".join(f'"{item}"' for item in value)
                query = query.filter(column, "in", f"({in_values})")
                
            else:
                print(f"Warning: Mixed types in list for column '{column}'. Skipping this filter.")
                
        elif value is None:
            query = query.filter(column, "is", "null")
            
        else:
            print(f"Warning: Unsupported type for column '{column}'. Skipping this filter.")
            
    return query



"""
# Different text search types
websearch_filter = CustomArgument(
    sign="text_search",
    value="python & (machine learning | AI)",
    config={"type": "websearch"}
)

phrase_search = CustomArgument(
    sign="text_search",
    value="exact phrase match",
    config={"type": "phrase"}
)

plain_search = CustomArgument(
    sign="text_search",
    value="simple text search",
    config={"type": "plain"}
)
"""

def search_across_tables_people_example():
    # Simple equality search
    result = search_across_tables_people(
        people_filters=[{"location": "New York"}],
        education_filters=[{"institution_name": "MIT"}],
        titles_filters=[{"company_name": "Google"}]
    )
    
    # Complex search with custom arguments
    result = search_across_tables_people(
        people_filters=[{
            "location": CustomArgument(sign="ilike", value="*emirates*")
        }],
        education_filters=[{
            "education_start_year": CustomArgument(sign="gte", value=2000),
            "institution_name": ["MIT", "Notre Dame University"]
        }],
        titles_filters=[{
            "title": CustomArgument(sign="ilike", value="*manager*"),
            "type": CustomArgument(sign="eq", value="in"),
            "year": CustomArgument(sign="gte", value=2001)
        }]
    )    
    
    result = search_across_tables_people(
    
        titles_filters=[{
            "title": CustomArgument(sign="ilike", value="*founder*"),
            "type": CustomArgument(sign="in", value=(['in','additional_title','promotion'])),
            "year": CustomArgument(sign="gte", value=2001)
        }]
    )    
    
    result = search_across_tables_people(
    
        titles_filters=[{
            "title": CustomArgument(sign="ilike", value="*manager*"),
            "company_experience_location":CustomArgument(sign="ilike", value="*frankfurt*"), 
            "type": CustomArgument(sign="in", value=(['additional_title','promotion'])),
            "year": CustomArgument(sign="gte", value=2001)
        }]
    )  
    
    ###usefull for API access ###
    
    #founder discovery: find founder (title (similar, not exact)) since a year and optional month in a location (similar, not exact)
    result = search_across_tables_people(
    
        titles_filters=[{
            "title": CustomArgument(sign="ilike", value="*founder*"),
            "company_experience_location":CustomArgument(sign="ilike", value="*Lebanon*"), 
            "type": CustomArgument(sign="in", value=(['in','additional_title','promotion'])),
            "year": CustomArgument(sign="gte", value=2012),
            "month": CustomArgument(sign="gte", value=1)
        }],#this below to limit to one result!
        range_from=0,
        range_to=0
    ) 
    
    
    #Startup Mafia: find founder (title (similar, not exact)) since a year and optional month in a location (similar, not exact), ex employee of a company by linkedin_handle_of_company
    result = search_across_tables_people(
    
        titles_filters=[{
            "title": CustomArgument(sign="ilike", value="*founder*"),
            "company_experience_location":CustomArgument(sign="ilike", value="*Lebanon*"), 
            "type": CustomArgument(sign="in", value=(['in','additional_title','promotion'])),
            "year": CustomArgument(sign="gte", value=2012),
            "month": CustomArgument(sign="gte", value=1)
        },
          {
              
              "linkedin_handle_of_company": CustomArgument(sign="eq", value="abc"),
              "type": CustomArgument(sign="eq", value="out")
            
          }  
            
            
            
            ],
        range_from=0,
        range_to=99
    ) 
    
    
    #alumni + founder + specific university by uni domain
    result = search_across_tables_people(
        titles_filters=[{
            "title": CustomArgument(sign="ilike", value="*founder*")}],
        
         education_filters=[{
        "institution_url": CustomArgument(sign="ilike", value="*lmu.de*")
    }]
)
    
    
    
    result = search_across_tables_people(
        
        education_filters=[{
            "institution_name": CustomArgument(sign="text_search", value="Dame", config={"type": "plain"})
           
        }]
    )    













