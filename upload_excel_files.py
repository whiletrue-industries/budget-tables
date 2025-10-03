import os
import json
import time
import random
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.errors import HttpError

def replace_tab_with_retry(spreadsheet_id, tab_name, filename, max_retries=3, base_delay=300):
    """
    Replace a tab with retry mechanism.
    Args:
        spreadsheet_id: The target spreadsheet ID
        tab_name: The name for the new tab
        filename: The Excel file to upload
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds (5 minutes = 300 seconds)
    """
    for attempt in range(max_retries + 1):
        try:
            print(f"Attempt {attempt + 1} of {max_retries + 1}")
            replace_tab(spreadsheet_id, tab_name, filename)
            return  # Success, exit the retry loop
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries:
                # Calculate delay with some jitter to avoid thundering herd
                delay = base_delay + random.uniform(0, 60)  # Add 0-60 seconds jitter
                print(f"Waiting {delay:.1f} seconds before retry...")
                time.sleep(delay)
            else:
                print("All retry attempts exhausted. Giving up.")
                raise


def replace_tab(spreadsheet_id, tab_name, filename):
    SERVICE_FILE   = "budget-tables-61600dd135e5.json"
    SCOPES         = ["https://www.googleapis.com/auth/drive",
                    "https://www.googleapis.com/auth/spreadsheets"]
    SOURCE_XLSX    = filename
    TARGET_SPREAD  = spreadsheet_id
    TARGET_TAB     = tab_name

    try:
        creds = json.loads(os.environ['CREDENTIALS_JSON'])
    except:
        print('BAD CREDENTIALS', os.environ.get('CREDENTIALS_JSON')[:20], '...', os.environ.get('CREDENTIALS_JSON')[-20:])
        raise
    with open(SERVICE_FILE, 'w') as f:
        json.dump(creds, f)

    creds   = Credentials.from_service_account_file(SERVICE_FILE, scopes=SCOPES)
    drive   = build("drive",   "v3", credentials=creds)
    sheets  = build("sheets",  "v4", credentials=creds)

    tmp_id = None
    try:
        # ── 1. Upload Excel and convert to a temporary Google Sheet ────────────
        print("PHASE 1: Uploading and converting Excel file to Google Sheets...")
        media = MediaFileUpload(
            SOURCE_XLSX,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            resumable=False,
        )
        file_meta = {
            "name": "tmp-xls-upload",
            "mimeType": "application/vnd.google-apps.spreadsheet"
        }
        tmp_id = drive.files().create(body=file_meta, media_body=media, fields="id").execute()["id"]
        print(f"Temporary spreadsheet created with ID: {tmp_id}")

        # the converted sheet usually has exactly one worksheet: grab its ID
        src_sheet_id = sheets.spreadsheets().get(
            spreadsheetId=tmp_id, fields="sheets.properties.sheetId"
        ).execute()["sheets"][0]["properties"]["sheetId"]
        print(f"Source sheet ID: {src_sheet_id}")

        # ── 2. Remove all sheets except the first one ─────────────────────
        print("PHASE 2: Cleaning up existing sheets...")
        dest_sheets = sheets.spreadsheets().get(
            spreadsheetId=TARGET_SPREAD, fields="sheets.properties"
        ).execute()["sheets"]
        
        # Keep the first sheet, delete all others
        sheets_to_delete = []
        first_sheet_id = None
        for i, sheet in enumerate(dest_sheets):
            sheet_id = sheet["properties"]["sheetId"]
            if i == 0:
                first_sheet_id = sheet_id
                print(f"Keeping first sheet: {sheet['properties'].get('title', 'Untitled')} (ID: {sheet_id})")
            else:
                sheets_to_delete.append(sheet_id)
                print(f"Will delete sheet: {sheet['properties'].get('title', 'Untitled')} (ID: {sheet_id})")

        # Delete all sheets except the first one
        if sheets_to_delete:
            requests = [{"deleteSheet": {"sheetId": sheet_id}} for sheet_id in sheets_to_delete]
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=TARGET_SPREAD, body={"requests": requests}
            ).execute()
            print(f"Deleted {len(sheets_to_delete)} sheets")

        # ── 3. Copy the new sheet into the destination spreadsheet ───────────────
        print("PHASE 3: Copying new sheet...")
        new_sheet_props = sheets.spreadsheets().sheets().copyTo(
            spreadsheetId=tmp_id,
            sheetId=src_sheet_id,
            body={"destinationSpreadsheetId": TARGET_SPREAD},
        ).execute()
        new_sheet_id = new_sheet_props["sheetId"]
        print(f"New sheet copied with ID: {new_sheet_id}")

        # ── 4. Rename the new sheet ─────────────────────
        print("PHASE 4: Renaming new sheet...")
        requests = [
            {"updateSheetProperties": {
                "properties": {"sheetId": new_sheet_id, "title": TARGET_TAB},
                "fields": "title"
            }}
        ]
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=TARGET_SPREAD, body={"requests": requests}
        ).execute()
        print(f"Sheet renamed to: {TARGET_TAB}")

        # ── 5. Get direct link to the new sheet (#gid=) ──────────────────
        new_sheet_url = f"https://docs.google.com/spreadsheets/d/{TARGET_SPREAD}/edit#gid={new_sheet_id}"
        print(f"New sheet URL: {new_sheet_url}")

        # write value in the target spreadsheet, first sheet cell N1
        try:
            sheets.spreadsheets().values().update(
                spreadsheetId=TARGET_SPREAD,
                range=f"הסבר!N1",
                valueInputOption="USER_ENTERED",
                body={"values": [[new_sheet_url]]}
            ).execute()
            print("Updated reference cell N1")
        except Exception as e:
            print(f"Warning: Could not update reference cell N1: {e}")

        print("Tab replaced successfully 🎉")

    finally:
        # ── 6. House-keeping: kill the temporary spreadsheet ──────────────────
        if tmp_id:
            try:
                drive.files().delete(fileId=tmp_id).execute()
                print("Temporary spreadsheet cleaned up")
            except Exception as e:
                print(f"Warning: Could not delete temporary spreadsheet {tmp_id}: {e}")


if __name__ == "__main__":
    import sys
    sheet_id = sys.argv[1]
    tab_name = sys.argv[2]
    filename = sys.argv[3]
    print(f"Replacing tab '{tab_name}' in spreadsheet '{sheet_id}' with '{filename}'")
    # Use the retry function instead of the direct function
    replace_tab_with_retry(sheet_id, tab_name, filename)
