import os
import csv
import json
import math

from time import sleep
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2 import service_account


SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

SERVICE_ACCOUNT_FILE = os.path.join(os.getcwd(), "service-account.json")

CREDENTIALS = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)


def write_to_sheets(
    supplier_gstin,
    gst_type,
    gst_rate,
    taxable_value,
    range_name,
    spreadsheet_id,
    supplier_name="",
):

    with build("sheets", "v4", credentials=CREDENTIALS) as service:
        values = [
            [
                f"{supplier_gstin}  {supplier_name}",
                gst_rate,
                gst_type,
                taxable_value,
            ],
        ]

        n = 0
        total_wait = 0
        max_backoff = 32
        deadline = 300

        while total_wait < deadline:
            try:
                result = (
                    service.spreadsheets()
                    .values()
                    .update(
                        spreadsheetId=spreadsheet_id,
                        range=range_name,
                        valueInputOption="USER_ENTERED",
                        body={"values": values},
                    )
                    .execute()
                )
                print(f"Success! {result.get('updatedCells')} cells updated.")
                break
            except HttpError:
                wait_time = min(math.pow(2, n), max_backoff)
                sleep(wait_time)
                total_wait = total_wait + wait_time
                n = n + 1


def gstr2acsv_to_google_sheets(gstr2a_file, sheet_name, spreadsheet_id, row_num):
    with open(
        gstr2a_file,
        mode="r",
        encoding="utf-8",
        newline="",
    ) as csvfile:

        csvreader = csv.DictReader(csvfile)

        for row in csvreader:
            if row["Integrated Tax"] == "0":
                gst_type = "INTRA"
            else:
                gst_type = "INTER"

            write_to_sheets(
                supplier_gstin=row["GSTIN of supplier"],
                gst_type=gst_type,
                gst_rate=row["Rate (%)"],
                taxable_value=row["Taxable Value"],
                range_name=f"{sheet_name}!A{row_num}:D{row_num}",
                spreadsheet_id=spreadsheet_id,
            )
            row_num = row_num + 1


def gstr2bjson_to_google_sheets(gstr2b_file, sheet_name, spreadsheet_id, row_num):
    with open(gstr2b_file, "r") as f:
        json_data = json.load(f)

    for b2b_supplier in json_data["data"]["docdata"]["b2b"]:
        supplier_gstin = b2b_supplier["ctin"]
        supplier_name = b2b_supplier["trdnm"]
        for inv in b2b_supplier["inv"]:
            for inv_item in inv["items"]:
                if inv_item["rt"] == 0:
                    continue

                if inv_item.get("igst") is not None and inv_item.get("igst") > 0:
                    gst_type = "INTER"
                else:
                    gst_type = "INTRA"

                write_to_sheets(
                    supplier_gstin=supplier_gstin,
                    supplier_name=supplier_name,
                    gst_type=gst_type,
                    gst_rate=inv_item["rt"],
                    taxable_value=inv_item["txval"],
                    range_name=f"{sheet_name}!A{row_num}:D{row_num}",
                    spreadsheet_id=spreadsheet_id,
                )
                row_num = row_num + 1


if __name__ == "__main__":
    gstr2a_file = "GSTR2A-INVOICE-22AEZPA0778G1ZF-102021-102021.csv"
    gstr2b_file = "returns_R2B_22AEZPA0778G1ZF_012022.json"

    spreadsheet_id = "1ZKTc8KJn5O7q7L8U2fUDXAYDHML0AmI7a5T8tWeKBr8"
    sheet_name = "January 2022"
    row_num = 8

    """ gstr2acsv_to_google_sheets(
        gstr2a_file, sheet_name, spreadsheet_id, row_num) """

    gstr2bjson_to_google_sheets(
        gstr2b_file, sheet_name, spreadsheet_id, row_num)
