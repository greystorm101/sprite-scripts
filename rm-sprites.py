import argparse
import datetime
import os
import pandas
import re
import time
import shutil

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =====CONFIGURE THE BELOW TO MATCH YOUR SETUP =======

REPO_PATH = "/path/to/customsprites"
GOOGLE_CREDS_FILE_PATH = "/path/to/credentials.json"
REMOVED_SPRITES_FOLDER = "/path/to/customsprites/Removed"

DEX_SPREADSHEET_ID = "spreadhseetIDHere"
DEX_RESPONSE_SHEET_ID = "sheetIDHere"
DEX_RESPONSE_SHEET_NAME = "RESPONSES"

CREDITS_SPREADSHEET_ID = "spreadhseetIDHere"
CREDITS_CREDIT_SHEET_ID = "sheetIDHere"
CREDITS_CREDIT_SHEET_NAME = "Credits"

NUM_SHEET_RETRIES = 5

# If true, will re-fetch the spreadsheets after every request. Much slower, but is more safe in case the sheet is currently active
TRUST_NO_CACHE=True

# ======= DO NOT MODIFY BELOW HERE =======

def user_sprite_deletion(username: str, include_collabs: bool = False, only_delete: list = None, preserve_data: bool = False):
    """
    Main loop that will iterate over a user's sprites and delete them from all needed resources
    """
    print(f"Removing sprites for user {username}...")
    # Open and read in the credit sheet first
    csv_file_path = os.path.join(REPO_PATH, 'Sprite Credits.csv')
    df = pandas.read_csv(csv_file_path, names=["filename", "author", "status", "tags"])
    
    # Collab sprites must be seperated so they be handled approriately. We will differentiate sole authorship sprites from collab sprites.
    all_user_rows = df[df["author"].str.contains(username, na=False)]
    no_collab_rows = df.loc[df["author"] == username]
    collab_rows = all_user_rows.drop(no_collab_rows.index)

    # We already run into a weird edge case here where a sprite can be credited as a collab between Game Freak or Pokémon TCG
    # if it is directly referencing official art. If that is the *only* collaborator we can safely remove that sprite.
    cleaned_collabs = collab_rows["author"].str.replace('é', 'e').str.lower()
    cleaned_username = username.lower().replace("é","e")
    non_user_collab_strings = [f"{cleaned_username} & game freak", f"game freak & {cleaned_username}", f"{cleaned_username} & pokemon tcg", f"pokemon tcg & {cleaned_username}"]
    non_user_collab_map = cleaned_collabs.str.contains('|'.join(non_user_collab_strings))

    # We now patch together our results and have dataframes for collabs and non-collabs
    non_user_collabs = collab_rows[non_user_collab_map]
    true_collabs = collab_rows[~non_user_collab_map]
    sole_author_rows = pandas.concat([no_collab_rows, non_user_collabs], axis=0)

    # Pull out file names and remove the files
    collab_files = true_collabs['filename'].tolist()
    sole_author_files = sole_author_rows['filename'].tolist()

    if include_collabs or only_delete is not None:
        sole_author_files.extend(collab_files)
    else:
        print(f"Collabs from this user that will NOT be automatically deleted:\n{collab_files}\n== You can re-run the script with -c to include all of these in the deletion, or run them through as individual files with the -o argument. ==")

    # If we are only deleting a subset, skip over sprites not included in that subset
    if only_delete is not None:
        sole_author_files = [item for item in sole_author_files if item in only_delete]
        missing_fusions = list(set(only_delete) - set(sole_author_files))
        if len(missing_fusions) > 0:
            print(f"WARNING: Supplied fusions not found by {username}:\n{missing_fusions}.\n")

    if preserve_data:
        print("Making backup...")
        make_backup(sole_author_files, username)

    print(f"-- Removing the following sprites for user {username}: --\n{sole_author_files}\n-----------")
    # Give the script runner some time to make sure there's no issues with the input before we start yeeting stuff
    time.sleep(5)

    # Cache our spreadsheets
    dex_response_sheet_cache = retry_sheet_operation(get_sprites_from_dex_response_sheet)
    credit_sheet_cache = retry_sheet_operation(get_sprites_from_credit_sheet)

    for fusion_id in sole_author_files:
        # Determine which file are affected and need to be bumped up a number after the removal of this current file
        regex = "^" + _fusion_name(fusion_id) + "[a-z]{0,2}"
        matching_files = df[df["filename"].str.fullmatch(regex)]
        matching_files_list = matching_files["filename"].tolist()

        _smaller_fusions, larger_fusions = split_list_on_removed_file(fusion_id, matching_files_list)

        # We delete first because it makes it easier to scootch the subsequent files back once it's already gone
        print(f"- Removing fusion {fusion_id}...")
        delete_fusion(fusion_id, dex_res_cache = dex_response_sheet_cache, credits_cache = credit_sheet_cache)
        
        # Bc im tired
        if TRUST_NO_CACHE:
            print("Refilling cache...")
            dex_response_sheet_cache_new = retry_sheet_operation(get_sprites_from_dex_response_sheet)
            credit_sheet_cache_new = retry_sheet_operation(get_sprites_from_credit_sheet)
            
            if dex_response_sheet_cache_new != dex_response_sheet_cache:
                print(f"Something has happened at {datetime.datetime.now()} and the dex cache is now wrong! Fixing...")
                dex_response_sheet_cache = dex_response_sheet_cache_new
            
            if credit_sheet_cache_new != credit_sheet_cache:
                print(f"Something has happened at {datetime.datetime.now()} and the credit cache is now wrong!! Fixing...")
                credit_sheet_cache = credit_sheet_cache_new


        if len(larger_fusions) > 0:
            print(f" * Fixing data for affected fusions: {larger_fusions}...")
            debump_fusions(larger_fusions, dex_res_cache = dex_response_sheet_cache, credits_cache = credit_sheet_cache)
            
            # Bc im tired
            if TRUST_NO_CACHE:
                print("Refilling cache...")
                dex_response_sheet_cache_new = retry_sheet_operation(get_sprites_from_dex_response_sheet)
                credit_sheet_cache_new = retry_sheet_operation(get_sprites_from_credit_sheet)
                
                if dex_response_sheet_cache_new != dex_response_sheet_cache:
                    print(f"Something has happened at {datetime.datetime.now()} and the dex cache is now wrong! Fixing...")
                    dex_response_sheet_cache = dex_response_sheet_cache_new
                
                if credit_sheet_cache_new != credit_sheet_cache:
                    print(f"Something has happened at {datetime.datetime.now()} and the credit cache is now wrong!! Fixing...")
                    credit_sheet_cache = credit_sheet_cache_new

            # I could do something smart here and update our cache, but it's pretty inexpensive to just re-read this file
            df = pandas.read_csv(csv_file_path, names=["filename", "author", "status", "tags"])

        # If a fusion by this author was modified just now, make sure we account for that
        if bool(set(larger_fusions) & set(sole_author_files)):
            for bumped_sprite in set(larger_fusions) & set(sole_author_files):
                bumped_sprite_index = sole_author_files.index(bumped_sprite) 
                sole_author_files[bumped_sprite_index] = bump_down_filename(bumped_sprite)
                print(f"        Removal affected {username}'s {bumped_sprite}. Adjusting to {bump_down_filename(bumped_sprite)}")
    
    print("Completed removals")        

def delete_fusion(fusion:str, dex_res_cache:list = None, dex_appr_cache:list = None, credits_cache:list = None):
    """
    Handles deleting the given fusion from all required resources (the file, credits spreadsheet, dex entry spreadsheet, ect)
    """
    # Delete the fusion from any google sheets resources
    #  Dex response sheet
    dex_result_rows_to_delete = find_sprite_in_dex_response_sheet(fusion, dex_res_cache)
    if len(dex_result_rows_to_delete) < 1:
        pass
    else:
        retry_sheet_operation(run_sheet_delete, DEX_SPREADSHEET_ID, DEX_RESPONSE_SHEET_ID, dex_result_rows_to_delete)
        # Update cache
        if dex_res_cache is not None:
            for row in dex_result_rows_to_delete:
                del dex_res_cache[row - 3]

    #  Credits sheet
    credits_rows_to_delete = find_sprite_in_credit_sheet(fusion , cache=credits_cache)
    if len(credits_rows_to_delete) < 1:
        print(f"WARNING: No rows in credit sheet found for fusion {fusion}. THIS SHOULD NOT HAPPEN.")
    else:
        retry_sheet_operation(run_sheet_delete, CREDITS_SPREADSHEET_ID, CREDITS_CREDIT_SHEET_ID, credits_rows_to_delete)
        # Update cache
        if credits_cache is not None:
            for row in credits_rows_to_delete:
                del credits_cache[row - 2]

    print(f"Removed: {dex_result_rows_to_delete} in dex responses; {credits_rows_to_delete} in credits")

    # Delete the file from the repo
    sprite_dir = os.path.join("Other", "BaseSprites") if not '.' in fusion else "CustomBattlers"
    fusion_file = os.path.join(REPO_PATH, sprite_dir, f"{fusion}.png")
    os.remove(fusion_file)

    # Remove the credit line in the repo's csv
    csv_file_path = os.path.join(REPO_PATH, 'Sprite Credits.csv')
    df = pandas.read_csv(csv_file_path, names=["filename", "author", "status", "tags"])
    df.drop(df.loc[df['filename'] == fusion].index, inplace=True)
    df.to_csv(csv_file_path, index=False, header=False)

def debump_fusions(fusion_list:str, dex_res_cache:list = None, dex_appr_cache:list = None, credits_cache:list = None):
    """
    Takes the fusion and decrements its filename by one everywhere
    """

    # To save us a lot of extra requests, we'll preform a single get for the sheet data we need and cache it
    if dex_res_cache is None:
        dex_res_cache = get_sprites_from_dex_response_sheet()
    if credits_cache is None:
        credits_cache = get_sprites_from_credit_sheet()

    # Also in an effort to save some requests, we'll collect the updates and run them in bulk at the end
    dex_response_update_data = []
    credits_update_data = []


    for fusion in fusion_list:
        new_fusion_name = bump_down_filename(fusion)

        # Move the file
        sprite_dir = os.path.join("Other", "BaseSprites") if not '.' in fusion else "CustomBattlers"
        fusion_file = os.path.join(REPO_PATH, sprite_dir, f"{fusion}.png")
        new_fusion_file = os.path.join(REPO_PATH, sprite_dir, f"{new_fusion_name}.png")

        if os.path.exists(new_fusion_file):
            temp_file_name = os.path.join(REPO_PATH, sprite_dir, f"{new_fusion_name}_temp.png")
            print(f"WARNING: Trying to move {fusion_file} to {new_fusion_file}, but it already exists.\nSaving {new_fusion_file} to {temp_file_name}")
            # Replace will silently replace existing file if one exists
            os.replace(new_fusion_file, temp_file_name)
        os.replace(fusion_file, new_fusion_file)

        # Modify the csv
        csv_file_path = os.path.join(REPO_PATH, 'Sprite Credits.csv')
        df = pandas.read_csv(csv_file_path, names=["filename", "author", "status", "tags"])
        df.loc[df['filename'] == fusion, 'filename'] = new_fusion_name
        df.to_csv(csv_file_path, index=False, header=False)

        # Modify the google sheets
        # Dex response
        dex_result_rows_affected = find_sprite_in_dex_response_sheet(fusion, dex_res_cache)
        if len(dex_result_rows_affected) < 1:
            pass
        else:
            update_data = {index:new_fusion_name for index in dex_result_rows_affected}
            update_body_data = make_sheet_update_data(DEX_RESPONSE_SHEET_NAME, update_data, "D")
            dex_response_update_data.extend(update_body_data)

            # Update cache
            if dex_res_cache is not None:
                for row in dex_result_rows_affected:
                    dex_res_cache[row - 3] = new_fusion_name

        # Credits
        credit_rows_affected = find_sprite_in_credit_sheet(fusion, credits_cache)
        if len(credit_rows_affected) < 1:
            print(f"WARNING: No rows in credit sheet found for fusion {fusion}. THIS SHOULD NOT HAPPEN.")
        else:
            update_data = {index:new_fusion_name for index in credit_rows_affected}
            update_body_data = make_sheet_update_data(CREDITS_CREDIT_SHEET_NAME, update_data, "D", needs_png=False)
            credits_update_data.extend(update_body_data)

            # Update cache
            if credits_cache is not None:
                for row in credit_rows_affected:
                    credits_cache[row - 2] = new_fusion_name  
    
    # Write to sheets
    retry_sheet_operation(run_sheet_update, DEX_SPREADSHEET_ID, dex_response_update_data)
    retry_sheet_operation(run_sheet_update, CREDITS_SPREADSHEET_ID, credits_update_data)
        

def make_backup(fusions:list, username:str):
    backup_user_dir = os.path.join(REMOVED_SPRITES_FOLDER, username)
    if not os.path.exists(os.path.join(backup_user_dir, "Other", "BaseSprites")):
        os.makedirs(os.path.join(backup_user_dir, "Other", "BaseSprites"))

    if not os.path.exists(os.path.join(backup_user_dir, "CustomBattlers")):
        os.makedirs(os.path.join(backup_user_dir, "CustomBattlers"))

    csv_file_path = os.path.join(REPO_PATH, 'Sprite Credits.csv')
    df = pandas.read_csv(csv_file_path, names=["filename", "author", "status", "tags"])
    new_df = pandas.DataFrame()

    for fusion in fusions: 
        sprite_dir = os.path.join("Other", "BaseSprites") if not '.' in fusion else "CustomBattlers"
        fusion_file = os.path.join(REPO_PATH, sprite_dir, f"{fusion}.png")
        backup_file = os.path.join(backup_user_dir, sprite_dir, f"{fusion}.png")
        shutil.copy(fusion_file, backup_file)

        # Remove the credit line in the repo's csv
        lil_df = df.loc[df['filename'] == fusion]
        new_df = pandas.concat([new_df, lil_df], axis=0)

    new_csv_file_path = os.path.join(backup_user_dir, 'Sprite Credits.csv')
    new_df.to_csv(new_csv_file_path, index=False, header=False)

# === Fusion name parsing helpers ===

def bump_down_filename(filename: str) -> str:
    """
    Takes a fusion name and decrements it (1.1c -> 1.1b)
    """
    trailing_chars = _fusion_trailing_chars(filename)
    version_num = letters_to_numeric(trailing_chars) - 1
    if version_num < 0:
        raise ValueError("Tried to bump down a file that was already base")

    return filename[:-len(trailing_chars)] + numeric_to_letters(version_num)


def split_list_on_removed_file(filename: str, fusions_list: list):
    """
    Splits a list into 2 lists, one with fusions with a higher "version id" than the target file,
    anf one lower
    """
    target_sprite_number = letters_to_numeric(_fusion_trailing_chars(filename))
    larger_files = [sprite_name for sprite_name in fusions_list if letters_to_numeric(_fusion_trailing_chars(sprite_name)) > target_sprite_number]
    smaller_files = [sprite_name for sprite_name in fusions_list if letters_to_numeric(_fusion_trailing_chars(sprite_name)) < target_sprite_number]

    return(smaller_files,larger_files)


def letters_to_numeric(letters:str) -> int:
    """
    Turns a string of lowercase letters a-z into a "base 26" (no zero)
    int representation where a=1, b=2, ect.
    """
    # First, turn our letters into a numberic string.
    # 'az' would be turned into '[1, 26]'
    num_list = [ord(char)-96 for char in letters]
    # Reverse it so the math with calculating the power is easier later
    num_list.reverse()

    final_number = 0
    for index in range(len(num_list)):
        final_number += num_list[index] * pow(26, index)
    return final_number


def numeric_to_letters(number:int) -> str:
    """
    Turns a "base 26" (no zero) int representation where a=1, b=2, ect.
    into a string of lowercase letters a-z (This is making an assumpiton
    that we won't wrap around to 3 letters, which should be safe to assume for the
    forseable future)
    """
    # Zero is a weird case, we're just going to manually handle that
    if number == 0:
        return ''
    
    first_letter = chr((number // 26) + 96)
    second_letter = chr((number % 26) + 96)

    # Because we are doing some cursed logic where 0 is not a character in our representation, we need
    # to handle z wraparounds semi-explicitly here (ex: c` will actuall be bz)
    if second_letter == '`':
        first_letter = chr( (number // 26) + 96 -1)
        second_letter = 'z'

    # Remove "leading zeros"
    if first_letter == '`':
        return second_letter

    return first_letter + second_letter


# === Google Sheets helpers ===

def get_sprites_from_dex_response_sheet() -> list:
    """
    Gets list of fusions in the dex response sheet
    """
    response_entry_range = f"{DEX_RESPONSE_SHEET_NAME}!D3:D"
    dex_results_entries = _flatten_fusion_list(
                _get_values_from_google_sheet(DEX_SPREADSHEET_ID, response_entry_range))
    return dex_results_entries


def find_sprite_in_dex_response_sheet(fusion:str, cache:list = None) -> list:
    """
    Returns rows that match a given fusion name in the dex response sheet
    """
    dex_results_entries = get_sprites_from_dex_response_sheet() if cache is None else cache
    num_headers = 2
    return _get_index_matching_items(fusion, dex_results_entries, num_headers)


def get_sprites_from_credit_sheet() -> list:
    """
    Gets list of fusions in the credits sheet
    """
    response_entry_range = "D2:D"
    dex_results_entries = _flatten_fusion_list(
                _get_values_from_google_sheet(CREDITS_SPREADSHEET_ID, response_entry_range))
    return dex_results_entries


def find_sprite_in_credit_sheet(fusion:str, cache:list = None) -> list:
    """
    Returns rows that match a given fusion name in the credits sheet
    """
    credit_entries = get_sprites_from_credit_sheet() if cache is None else cache
    num_headers = 1
    return _get_index_matching_items(fusion, credit_entries, num_headers)


def _get_values_from_google_sheet(spreadsheet_id: str, sheet_range: str) -> list:
    """
    Performs a get for the given range on a google sheet
    """
    creds = _get_google_creds()
    try:
        service = build("sheets", "v4", credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = (
            sheet.values()
            .get(spreadsheetId=spreadsheet_id, range=sheet_range)
            .execute()
        )
        
        return result["values"]

    except HttpError as err:
        print(err)
        raise HttpError


def run_sheet_delete(spreadsheet_id: str, sheet_id: str, del_rows: list):
    """
    Deletes a list of rows on a given sheet
    """
    creds = _get_google_creds()

    def make_del_dim(row: int):
        del_dim = {
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": row-1,
                    "endIndex": row
                }
            }
        }
        return del_dim
    
    # Infuriatingly, we must make sure we delete backwards since it will process each delete one at a time
    del_rows.sort(reverse=True)
    del_requests = [make_del_dim(i) for i in del_rows]
    del_body = {"requests": del_requests}

    try:
        service = build("sheets", "v4", credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = (
            sheet.batchUpdate(spreadsheetId=spreadsheet_id, body=del_body).execute()
        )
        
        return result

    except HttpError as err:
        print(err)
        raise HttpError

def make_sheet_update_data(sheet_name: str, update_rows: dict, col_letter:str, needs_png: bool = True):
    """
    Updates a cell in the given sheet
    """
    def make_update_dim(row: int, value:str):
        cell_value = f"{value}.png" if needs_png else value
        update_data =  {
            "range": f"{sheet_name}!{col_letter}{row}",
            "majorDimension": "COLUMNS",
            "values": [
                [cell_value]
            ]
        }
        return update_data
    
    update_requests = [make_update_dim(i,v) for i,v in update_rows.items()]
    return update_requests


def run_sheet_update(spreadsheet_id: str, update_requests: list):
    """
    Updates a cell in the given sheet
    """
    creds = _get_google_creds()

    value_input_option = "USER_ENTERED"
    body = {"valueInputOption": value_input_option, "data": update_requests}

    try:
        service = build("sheets", "v4", credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = (sheet.values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute())
        
        return result

    except HttpError as err:
        print(err)
        raise HttpError


def retry_sheet_operation(fun, *args):
    retries = NUM_SHEET_RETRIES
    while retries > 0:
        try:
            return fun(*args)
        except BaseException as e:
            retries -= 1
            if retries == 0:
                raise e
            else:
                print(f"Error running sheet update: {e}. Retrying...")
                time.sleep(10)


# === Private helpers ===

def _fusion_name(sprite_name: str) -> str:
    fusion_name_regex = r'^[0-9]+\.*[0-9]*'
    return re.findall(fusion_name_regex, sprite_name)[0]


def _fusion_trailing_chars(sprite_name: str) -> str:
    """
    This will turn the sprite file names '1.59', '1.59a', '1.59ab' into a list of the trailing letter chars '`', 'a', 'ab'
    We use ` for no trailing chars because it is one unicode character below a, so it will evaluate to 0
    """
    trailing_chars_regex = r'[a-z]{1,2}'
    return re.findall(trailing_chars_regex, sprite_name)[0] if len(re.findall(trailing_chars_regex, sprite_name)) > 0 else '`'



def _flatten_fusion_list(fusion_list: list) -> list:
    """
    Flattesns nested list into a list of fusion names without training .png
    """
    return [x[:-4] if x[-4:] == ".png" else x 
            for xs in fusion_list for x in xs]
    

def _get_index_matching_items(item:str, lst: list, offset:int = 0)-> list:
    """
    Given a list and a target item, returns a list of indicies that contain target item
    """
    matched_indexes = []
    for index,itm in enumerate(lst):
        if itm == item:
            matched_indexes.append(index + offset + 1)
            
    return matched_indexes


def _get_google_creds() -> Credentials:
    """
    Fetches creds to access spreadsheets with 
    """
    creds = None
    SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

    creds_root_path = os.path.dirname(GOOGLE_CREDS_FILE_PATH)
    token_path = os.path.join(creds_root_path, "token.json")
    
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                GOOGLE_CREDS_FILE_PATH, SCOPES
            )
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_path, "w") as token:
            token.write(creds.to_json())

    return creds


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
                    prog='SpriteEraser',
                    description='Removes all sprites/credits/ect for a given user.')
    parser.add_argument('username')
    parser.add_argument('-c', '--collabs', action='store_true', help='If flag is set, will also remove all collab sprites', required=False)
    parser.add_argument('-o', '--only', nargs='+',  help='List of sprites to remove. If this is supplied, will not remove any other sprites outside of those provided.', required=False)
    parser.add_argument('-b', '--backup',  action='store_true', help='If flag is set, will save backup of user sprite info before removing', required=False)
    
    args = parser.parse_args()
    user_sprite_deletion(args.username, args.collabs, args.only, args.backup)
