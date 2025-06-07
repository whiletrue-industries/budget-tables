import os
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

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
        print('BAD CREDENTAIALS', os.environ.get('CREDENTIALS_JSON')[:20], '...', os.environ.get('CREDENTIALS_JSON')[-20:])
        raise
    with open(SERVICE_FILE, 'w') as f:
        json.dump(creds, f)

    creds   = Credentials.from_service_account_file(SERVICE_FILE, scopes=SCOPES)
    drive   = build("drive",   "v3", credentials=creds)
    sheets  = build("sheets",  "v4", credentials=creds)

    # ── 1. Upload Excel and convert to a temporary Google Sheet ────────────
    print("PHASE 1: Uploading and converting Excel file to Google Sheets...")
    media = MediaFileUpload(
        SOURCE_XLSX,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        resumable=False,
    )
    file_meta = {
        "name": "tmp-xls-upload",
        "mimeType": "application/vnd.google-apps.spreadsheet"  # triggers conversion :contentReference[oaicite:1]{index=1}
    }
    tmp_id = drive.files().create(body=file_meta, media_body=media, fields="id").execute()["id"]
    print(f"Temporary spreadsheet created with ID: {tmp_id}")

    # the converted sheet usually has exactly one worksheet: grab its ID
    src_sheet_id = sheets.spreadsheets().get(
        spreadsheetId=tmp_id, fields="sheets.properties.sheetId"
    ).execute()["sheets"][0]["properties"]["sheetId"]
    print(f"Source sheet ID: {src_sheet_id}")

    # ── 2. Delete the old tab  ─────────────────────
    # find existing tab’s sheetId by name
    dest_sheets = sheets.spreadsheets().get(
        spreadsheetId=TARGET_SPREAD, fields="sheets.properties"
    ).execute()["sheets"]
    old_sheet_id = next(
        s["properties"]["sheetId"] for s in dest_sheets
        if s["properties"]["title"] == TARGET_TAB
    )

    requests = [
        {"deleteSheet": {"sheetId": old_sheet_id}},
    ]
    sheets.spreadsheets().batchUpdate(
        spreadsheetId=TARGET_SPREAD, body={"requests": requests}
    ).execute()

    # ── 3. Copy that sheet into the destination spreadsheet ───────────────
    new_sheet_props = sheets.spreadsheets().sheets().copyTo(
        spreadsheetId=tmp_id,
        sheetId=src_sheet_id,
        body={"destinationSpreadsheetId": TARGET_SPREAD},
    ).execute()
    new_sheet_id = new_sheet_props["sheetId"]

    # ── 4. rename the new copy ─────────────────────
    requests = [
        {"updateSheetProperties": {
            "properties": {"sheetId": new_sheet_id, "title": TARGET_TAB},
            "fields": "title"
        }}
    ]
    sheets.spreadsheets().batchUpdate(
        spreadsheetId=TARGET_SPREAD, body={"requests": requests}
    ).execute()

    # ── 5. House-keeping: kill the temporary spreadsheet ──────────────────
    drive.files().delete(fileId=tmp_id).execute()

    # ── 6. Get direct link to the new sheet (#gid=) ──────────────────
    new_sheet_url = f"https://docs.google.com/spreadsheets/d/{TARGET_SPREAD}/edit#gid={new_sheet_id}"
    print(f"New sheet URL: {new_sheet_url}")

    # write value in the target spreadsheet, first sheet cell N1
    sheets.spreadsheets().values().update(
        spreadsheetId=TARGET_SPREAD,
        range=f"הסבר!N1",
        valueInputOption="USER_ENTERED",
        body={"values": [[new_sheet_url]]}
    ).execute()

    print("Tab replaced successfully 🎉")




if __name__ == "__main__":
    import sys
    sheet_id = sys.argv[1]
    tab_name = sys.argv[2]
    filename = sys.argv[3]
    print(f"Replacing tab '{tab_name}' in spreadsheet '{sheet_id}' with '{filename}'")
    replace_tab(sheet_id, tab_name, filename)