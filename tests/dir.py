import os
# Creates folders from a list of folder paths (relative or absolute).
def create_folders(folders):
    for path in folders:
        try:
            os.makedirs(path, exist_ok=True)
            print(f"✅ Created or exists: {path}")
        except Exception as e:
            print(f"❌ Error creating {path}: {e}")

if __name__ == "__main__":
    folders = [
        "etl",
        "etl/diagnostics",
        "etl/save_utils",
        "data/raw",
        "data/processed",
        "data/processed/silver",
        "data/processed/gold",
        "data/output",
        "docs",
        "credentials",
        "configs",
        "tests"
    ]
    create_folders(folders)