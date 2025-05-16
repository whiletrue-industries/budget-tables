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

    # ── 2. Copy that sheet into the destination spreadsheet ───────────────
    new_sheet_props = sheets.spreadsheets().sheets().copyTo(
        spreadsheetId=tmp_id,
        sheetId=src_sheet_id,
        body={"destinationSpreadsheetId": TARGET_SPREAD},
    ).execute()
    new_sheet_id = new_sheet_props["sheetId"]

    # ── 3. Delete the old tab and rename the new copy ─────────────────────
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
        {"updateSheetProperties": {
            "properties": {"sheetId": new_sheet_id, "title": TARGET_TAB},
            "fields": "title"
        }}
    ]
    sheets.spreadsheets().batchUpdate(
        spreadsheetId=TARGET_SPREAD, body={"requests": requests}
    ).execute()

    # ── 4. House-keeping: kill the temporary spreadsheet ──────────────────
    drive.files().delete(fileId=tmp_id).execute()

    print("Tab replaced successfully 🎉")




if __name__ == "__main__":
    import sys
    sheet_id = sys.argv[1]
    tab_name = sys.argv[2]
    filename = sys.argv[3]
    print(f"Replacing tab '{tab_name}' in spreadsheet '{sheet_id}' with '{filename}'")
    replace_tab(sheet_id, tab_name, filename)