import os
import json
import time
import random
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Staging tabs are named with this prefix, so leftovers from a failed run can be found and removed.
STAGING_PREFIX = "__staging__"

SHEET_FIELDS = (
    "sheets(properties,rowGroups,columnGroups,"
    "data(rowMetadata(pixelSize,hiddenByUser),columnMetadata(pixelSize,hiddenByUser)))"
)


def replace_tab_with_retry(spreadsheet_id, tab_name, filename, max_retries=3, base_delay=300):
    """
    Replace a tab with retry mechanism.
    Args:
        spreadsheet_id: The target spreadsheet ID
        tab_name: The name of the tab to update
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


def get_services():
    SERVICE_FILE = "budget-tables-61600dd135e5.json"
    SCOPES = ["https://www.googleapis.com/auth/drive",
              "https://www.googleapis.com/auth/spreadsheets"]
    if 'CREDENTIALS_JSON' in os.environ:
        try:
            creds = json.loads(os.environ['CREDENTIALS_JSON'])
        except:
            print('BAD CREDENTIALS', os.environ.get('CREDENTIALS_JSON')[:20], '...', os.environ.get('CREDENTIALS_JSON')[-20:])
            raise
        with open(SERVICE_FILE, 'w') as f:
            json.dump(creds, f)

    creds = Credentials.from_service_account_file(SERVICE_FILE, scopes=SCOPES)
    return build("drive", "v3", credentials=creds), build("sheets", "v4", credentials=creds)


def dimension_requests(sheet_id, dimension, metadata):
    """updateDimensionProperties requests reproducing sizes / hidden state, merged into runs of equal values."""
    requests = []
    start = 0
    for i in range(1, len(metadata) + 1):
        if i < len(metadata) and metadata[i] == metadata[start]:
            continue
        props = {"pixelSize": metadata[start].get("pixelSize", 100 if dimension == "COLUMNS" else 21),
                 "hiddenByUser": metadata[start].get("hiddenByUser", False)}
        requests.append({"updateDimensionProperties": {
            "range": {"sheetId": sheet_id, "dimension": dimension, "startIndex": start, "endIndex": i},
            "properties": props,
            "fields": "pixelSize,hiddenByUser",
        }})
        start = i
    return requests


def retarget(dim_range, sheet_id):
    return {**dim_range, "sheetId": sheet_id}


def build_refresh_requests(target, staging):
    """
    Requests that make the `target` tab look like the `staging` tab, without deleting the target.
    Keeps the target's sheetId (gid), name, position, and anything attached to it
    (charts, protected ranges, filter views, comments, formulas pointing at it).
    """
    t_props, s_props = target["properties"], staging["properties"]
    t_id, s_id = t_props["sheetId"], s_props["sheetId"]
    t_grid, s_grid = t_props["gridProperties"], s_props["gridProperties"]
    s_rows, s_cols = s_grid["rowCount"], s_grid["columnCount"]
    rows = max(t_grid["rowCount"], s_rows)
    cols = max(t_grid["columnCount"], s_cols)

    requests = []

    # 1. Remove old row/column groups (deepest first)
    for key, dimension in (("rowGroups", "ROWS"), ("columnGroups", "COLUMNS")):
        for g in sorted(target.get(key, []), key=lambda g: -g["depth"]):
            requests.append({"deleteDimensionGroup": {"range": retarget(g["range"], t_id)}})

    # 2. Grow the grid if needed (never shrink, so formulas pointing at the tab don't break),
    #    and copy over the sheet-level view settings
    requests.append({"updateSheetProperties": {
        "properties": {
            "sheetId": t_id,
            "rightToLeft": s_props.get("rightToLeft", False),
            "gridProperties": {
                "rowCount": rows,
                "columnCount": cols,
                "frozenRowCount": s_grid.get("frozenRowCount", 0),
                "frozenColumnCount": s_grid.get("frozenColumnCount", 0),
                "rowGroupControlAfter": s_grid.get("rowGroupControlAfter", False),
                "columnGroupControlAfter": s_grid.get("columnGroupControlAfter", False),
            },
        },
        "fields": "rightToLeft,gridProperties(rowCount,columnCount,frozenRowCount,frozenColumnCount,"
                  "rowGroupControlAfter,columnGroupControlAfter)",
    }})

    # 3. Wipe the old contents: merges, values, formats, notes, validation
    whole = {"sheetId": t_id, "startRowIndex": 0, "endRowIndex": rows,
             "startColumnIndex": 0, "endColumnIndex": cols}
    requests.append({"unmergeCells": {"range": whole}})
    requests.append({"updateCells": {"range": whole, "fields": "*"}})

    # 4. Paste the new contents (values, formulas, formats, notes, merges)
    requests.append({"copyPaste": {
        "source": {"sheetId": s_id, "startRowIndex": 0, "endRowIndex": s_rows,
                   "startColumnIndex": 0, "endColumnIndex": s_cols},
        "destination": {"sheetId": t_id, "startRowIndex": 0, "endRowIndex": s_rows,
                        "startColumnIndex": 0, "endColumnIndex": s_cols},
        "pasteType": "PASTE_NORMAL",
        "pasteOrientation": "NORMAL",
    }})

    # 5. Column widths, row heights and hidden rows/columns; leftover rows/columns beyond the
    #    new data get the defaults
    data = staging["data"][0]
    col_meta = data.get("columnMetadata", [])[:s_cols] + [{}] * (cols - s_cols)
    row_meta = data.get("rowMetadata", [])[:s_rows] + [{}] * (rows - s_rows)
    requests += dimension_requests(t_id, "COLUMNS", col_meta)
    requests += dimension_requests(t_id, "ROWS", row_meta)

    # 6. Recreate groups. Adding them shallowest first means a range inside an
    #    existing group gets nested one level deeper, matching the original depth.
    for key in ("rowGroups", "columnGroups"):
        groups = sorted(staging.get(key, []), key=lambda g: g["depth"])
        for g in groups:
            requests.append({"addDimensionGroup": {"range": retarget(g["range"], t_id)}})
        for g in groups:
            if g.get("collapsed"):
                requests.append({"updateDimensionGroup": {
                    "dimensionGroup": {"range": retarget(g["range"], t_id), "depth": g["depth"], "collapsed": True},
                    "fields": "collapsed",
                }})

    # 7. Drop the staging tab
    requests.append({"deleteSheet": {"sheetId": s_id}})
    return requests


def replace_tab(spreadsheet_id, tab_name, filename):
    drive, sheets = get_services()

    tmp_id = None
    staging_id = None
    try:
        # ── 1. Upload Excel and convert to a temporary Google Sheet ────────────
        print("PHASE 1: Uploading and converting Excel file to Google Sheets...")
        media = MediaFileUpload(
            filename,
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

        # ── 2. Remove staging tabs left over from failed runs ─────────────────
        print("PHASE 2: Cleaning up leftover staging tabs...")
        dest_sheets = sheets.spreadsheets().get(
            spreadsheetId=spreadsheet_id, fields="sheets.properties(sheetId,title)"
        ).execute()["sheets"]
        stale = [s["properties"]["sheetId"] for s in dest_sheets
                 if s["properties"]["title"].startswith(STAGING_PREFIX)]
        if stale:
            sheets.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": [{"deleteSheet": {"sheetId": sid}} for sid in stale]}
            ).execute()
            print(f"Deleted {len(stale)} leftover staging tabs")

        # ── 3. Copy the new sheet into the destination spreadsheet as a staging tab ──
        print("PHASE 3: Copying new sheet into a staging tab...")
        staging_id = sheets.spreadsheets().sheets().copyTo(
            spreadsheetId=tmp_id,
            sheetId=src_sheet_id,
            body={"destinationSpreadsheetId": spreadsheet_id},
        ).execute()["sheetId"]
        sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body={"requests": [
            {"updateSheetProperties": {
                "properties": {"sheetId": staging_id, "title": f"{STAGING_PREFIX}{int(time.time())}", "hidden": True},
                "fields": "title,hidden",
            }}
        ]}).execute()
        print(f"Staging tab ID: {staging_id}")

        # ── 4. Refresh the target tab in place (or create it on the first run) ──
        all_sheets = sheets.spreadsheets().get(
            spreadsheetId=spreadsheet_id, fields=SHEET_FIELDS
        ).execute()["sheets"]
        by_id = {s["properties"]["sheetId"]: s for s in all_sheets}
        staging = by_id[staging_id]
        target = next((s for s in all_sheets if s["properties"]["title"] == tab_name), None)

        if target is None:
            print(f"PHASE 4: No tab named '{tab_name}' yet, turning the staging tab into it...")
            requests = [{"updateSheetProperties": {
                "properties": {"sheetId": staging_id, "title": tab_name, "hidden": False},
                "fields": "title,hidden",
            }}]
            target_id = staging_id
        else:
            target_id = target["properties"]["sheetId"]
            print(f"PHASE 4: Refreshing tab '{tab_name}' (ID: {target_id}) in place...")
            requests = build_refresh_requests(target, staging)

        # A single batchUpdate is atomic: either the tab is fully refreshed or nothing changes
        sheets.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id, body={"requests": requests}
        ).execute()
        staging_id = None
        print(f"Applied {len(requests)} updates")

        # ── 5. Direct link to the tab (#gid=), stays the same between runs ──
        sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={target_id}"
        print(f"Sheet URL: {sheet_url}")

        try:
            sheets.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range="הסבר!N1",
                valueInputOption="USER_ENTERED",
                body={"values": [[sheet_url]]}
            ).execute()
            print("Updated reference cell N1")
        except Exception as e:
            print(f"Warning: Could not update reference cell N1: {e}")

        print("Tab replaced successfully 🎉")

    finally:
        # ── 6. House-keeping: kill the staging tab and the temporary spreadsheet ──
        if staging_id is not None:
            try:
                sheets.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id,
                    body={"requests": [{"deleteSheet": {"sheetId": staging_id}}]}
                ).execute()
                print("Staging tab cleaned up")
            except Exception as e:
                print(f"Warning: Could not delete staging tab {staging_id}: {e}")
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
