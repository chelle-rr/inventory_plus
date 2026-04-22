import os
import re
import hashlib
import pandas as pd
from datetime import datetime
import subprocess

try:
    import magic
    USE_MAGIC = True
except ImportError:
    import mimetypes
    USE_MAGIC = False

DO_HASH = False

def get_md5(file_path):
    try:
        result = subprocess.run(
            ["md5sum", file_path],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.split()[0]
    except Exception:
        return None


def get_mime_type(file_path):
    if USE_MAGIC:
        try:
            return magic.from_file(file_path, mime=True)
        except Exception:
            return None
    else:
        mime, _ = mimetypes.guess_type(file_path)
        return mime


def scan_directory(root):
    records = []

    for dirpath, dirnames, filenames in os.walk(root):
        for name in filenames:
            full_path = os.path.join(dirpath, name)

            try:
                size = os.path.getsize(full_path)
                mtime = os.path.getmtime(full_path)
                modified_date = datetime.fromtimestamp(mtime)
            except Exception:
                size = None
                modified_date = None

            mime = get_mime_type(full_path)
            md5 = get_md5(full_path) if DO_HASH else None

            records.append({
                "file_name": name,
                "file_path": full_path,
                "file_size": size,
                "mime_type": mime,
                "last_modified": modified_date,
                "md5": md5,
                "folder": dirpath
            })

    return pd.DataFrame(records)


def analyze(df):
    # total stats
    total_files = len(df)
    total_size = df["file_size"].sum()

    # duplicates
    if df["md5"].notna().any():
        # if generating checksum, use this for duplicate check
        dup_key = "md5"
        duplicate_method = "md5"

    else:
        # if not generating a checksum, match duplicates via size and name
        dup_key = ["file_size", "file_name"]
        duplicate_method = "file_size + file_name (approximate)"

    dup_groups = df[df.duplicated(dup_key, keep=False)]

    duplicate_files = len(dup_groups)
    duplicate_groups = (
        dup_groups[dup_key[0] if isinstance(dup_key, list) else dup_key]
        .nunique()
    )

    # mime stats
    mime_stats = df.groupby("mime_type").agg(
        file_count=("file_name", "count"),
        total_size=("file_size", "sum")
    ).reset_index().sort_values("file_count", ascending=False)

    # folder stats
    folder_stats = df.groupby("folder").agg(
        file_count=("file_name", "count"),
        total_size=("file_size", "sum")
    ).reset_index().sort_values("file_count", ascending=False)

    # date analysis
    df["year"] = df["last_modified"].dt.year
    min_year = df["year"].min()
    max_year = df["year"].max()
    mean_year = int(df["year"].mean()) if not df["year"].isna().all() else None

    summary = pd.DataFrame([{
        "total_files": total_files,
        "total_size_bytes": total_size,
        "duplicate_files": duplicate_files,
        "duplicate_groups": duplicate_groups,
        "duplicate_method": duplicate_method,
        "min_year": min_year,
        "max_year": max_year,
        "mean_year": mean_year
    }])

    return summary, mime_stats, folder_stats, dup_groups


def export_to_excel(df, summary, mime_stats, folder_stats, dup_groups, output_file):
    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="Full Inventory", index=False)
        summary.to_excel(writer, sheet_name="Summary", index=False)
        mime_stats.to_excel(writer, sheet_name="By MIME Type", index=False)
        folder_stats.to_excel(writer, sheet_name="By Folder", index=False)
        dup_groups.to_excel(writer, sheet_name="Duplicates", index=False)

def normalize_path(path):
    path = path.strip().strip('"')

    # convert Windows-style slashes to Unix-style (safe on all platforms)
    path = path.replace("\\", "/")

    # handle drive letter (e.g., M:/ → /mnt/m/)
    match = re.match(r"^([A-Za-z]):/(.*)", path)
    if match:
        drive = match.group(1).lower()
        rest = match.group(2)
        path = f"/mnt/{drive}/{rest}"

    # normalize for current OS (this will fix slashes appropriately)
    path = os.path.normpath(path)

    return path

if __name__ == "__main__":
    # prompt for directory
    while True:
        root_folder_input = input("Enter the directory to scan: ")
        root_folder = normalize_path(root_folder_input) 
        if os.path.isdir(root_folder):
            break
        else:
            print("That path doesn't exist or isn't a directory. Try again!\n")

    # prompt for output file (will create in current directory unless specified)
    output_excel = input("Enter output Excel filename with optional full path (default: inventory_report.xlsx): ").strip()

    if not output_excel:
        output_excel = "inventory_report.xlsx"

    if not output_excel.lower().endswith(".xlsx"):
        output_excel += ".xlsx"

    print("\nScanning directory ...\n")

    df = scan_directory(root_folder)

    print("Analyzing data ...\n")
    summary, mime_stats, folder_stats, dup_groups = analyze(df)

    print("Writing Excel report ...\n")
    export_to_excel(df, summary, mime_stats, folder_stats, dup_groups, output_excel)

    print(f"Done! Excel report created: {output_excel}")
