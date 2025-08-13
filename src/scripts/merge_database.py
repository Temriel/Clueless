# This file exists to merge SQLite3 database files according to the Clueless format
# in case of file corruption (what happened with the old database)
# Should this happen again, this script can probably be modified
import sqlite3
import os
import subprocess
from datetime import datetime, timedelta 
import json

# paths for the database files
DB_MALFORMED = "database_malformed.db"
DB_NEW = "database_new.db"
DB_MERGED = "database_merged.db"
RECOVERED_SQL = "recovered.sql"

TABLE_RULES = {
    "template": {
        "primary_key": "id",
        "foreign_keys": {
            "owner_id": "discord_user.discord_id"
        }
    },
    "template_stat": {
        "primary_key": ["template_id", "datetime"],
        "foreign_keys": {
            "template_id": "template.id"
        }
    },
    "template_manager": {
        "primary_key": ["template_id", "user_id"],
        "foreign_keys": {
            "template_id": "template.id",
            "user_id": "discord_user.discord_id"
        }
    },
    "discord_user": {
        "primary_key": "discord_id",
        "foreign_keys": {
            "pxls_user_id": "pxls_user.pxls_user_id"
        }
    },
    "pxls_user": {
        "primary_key": "pxls_user_id",
        "foreign_keys": {}
    },
    "pxls_name": {
        "primary_key": "pxls_name_id",
        "foreign_keys": {
            "pxls_user_id": "pxls_user.pxls_user_id"
        }
    },
    "server": {
        "primary_key": "server_id",
        "foreign_keys": {}
    },
    "server_pxls_user": {
        "primary_key": ["pxls_user_id", "server_id"],
        "foreign_keys": {
            "pxls_user_id": "pxls_user.pxls_user_id",
            "server_id": "server.server_id"
        }
    },
    "log_key": {
        "primary_key": ["discord_id", "canvas_code"],
        "foreign_keys": {
            "discord_id": "discord_user.discord_id"
        }
    },
    "pxls_general_stat": {
        "primary_key": ["stat_name", "datetime"],
        "foreign_keys": {}
    },
    "record": {
        "primary_key": "record_id",
        "foreign_keys": {}
    },
    "pxls_user_stat": {
        "primary_key": ["record_id", "pxls_name_id"],
        "foreign_keys": {
            "record_id": "record.record_id",
            "pxls_name_id": "pxls_name.pxls_name_id"
        }
    },
    "palette_color": {
        "primary_key": ["canvas_code", "color_id"],
        "foreign_keys": {}
    },
    "color_stat": {
        "primary_key": ["record_id", "color_id"],
        "foreign_keys": {
            "record_id": "record.record_id"
        }
    },
    "snapshot": {
        "primary_key": "datetime",
        "foreign_keys": {}
    },
    "canvas": {
        "primary_key": "canvas_code",
        "foreign_keys": {}
    }
}

def recover_sql_data():
    print("Attempting recovery using SQLite recovery extension...")
    # This function remains unchanged.
    result = subprocess.run([
        "sqlite3", DB_MALFORMED,
        ".load recover",
        ".mode insert",
        f".output {RECOVERED_SQL}",
        "SELECT * FROM recover();",
        ".exit"
    ], input="", text=True, capture_output=True)

    if result.returncode != 0 or not os.path.exists(RECOVERED_SQL):
        print(".sql file missing or recovery failed")
        return False
    print("SQL executed successfully")
    return True

def extract_and_copy_data(source_db, target_db, table_rules, batch_size=10000):
    """Extract data from the malformed DB and copy it to the target."""
    src_conn = sqlite3.connect(source_db)
    tgt_conn = sqlite3.connect(target_db)
    src_cur = src_conn.cursor()
    tgt_cur = tgt_conn.cursor()

    for table in table_rules:
        try:
            src_cur.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in src_cur.fetchall()]
            col_str = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(columns))
            src_cur.execute(f"SELECT * FROM {table}")
            while True:
                rows = src_cur.fetchmany(batch_size)
                if not rows:
                    break
                tgt_cur.executemany(
                    f"INSERT OR IGNORE INTO {table} ({col_str}) VALUES ({placeholders})", rows
                )
        except Exception as e:
            print(f"Error copying table {table}: {e}")

    tgt_conn.commit()
    src_conn.close()
    tgt_conn.close()
    print("Data extraction from DB_MALFORMED complete.")

def load_recovered_sql(db_path, sql_file):
    """Load the recovered SQL into a new SQLite database."""
    if not os.path.exists(sql_file):
        print(f"Recovered SQL file '{sql_file}' does not exist.")
        return False
    with sqlite3.connect(db_path) as conn, open(sql_file, 'r', encoding='utf-8') as f:
        sql_script = f.read()
        try:
            conn.executescript(sql_script)
            print(f"Loaded recovered data into {db_path}")
            return True
        except sqlite3.DatabaseError as e:
            print(f"Error loading recovered SQL: {e}")
            return False

def copy_schema(source_db, target_db):
    """Copy the database schema from one file to another."""
    with sqlite3.connect(source_db) as src, sqlite3.connect(target_db) as tgt:
        src.row_factory = sqlite3.Row
        cur = src.execute("SELECT sql FROM sqlite_master WHERE type='table'")
        for row in cur:
            if row['sql']:
                tgt.execute(row['sql'])
        tgt.commit()

def load_table_sql(sql_file, target_db):
    """Load a specific table from a .sql file."""
    if not os.path.exists(sql_file):
        print(f"SQL file '{sql_file}' does not exist.")
        return False
    with sqlite3.connect(target_db) as conn, open(sql_file, 'r', encoding='utf-8') as f:
        sql_script = f.read()
        try:
            conn.executescript(sql_script)
            print(f"Loaded table from {sql_file} into {target_db}")
            return True
        except sqlite3.DatabaseError as e:
            print(f"Error loading table SQL: {e}")
            return False

def build_pxls_name_mapping(db_new_path, db_merged_path):
    """
    Maps pxls_name_id & pxls_user_id between databases
    """
    new_conn = sqlite3.connect(db_new_path)
    merged_conn = sqlite3.connect(db_merged_path)
    new_cur = new_conn.cursor()
    merged_cur = merged_conn.cursor()

    new_cur.execute("SELECT pxls_name_id, name, pxls_user_id FROM pxls_name")
    new_names = {name: (pxls_name_id, pxls_user_id) for pxls_name_id, name, pxls_user_id in new_cur.fetchall()}
    merged_cur.execute("SELECT pxls_name_id, name, pxls_user_id FROM pxls_name")
    merged_names = {name: (pxls_name_id, pxls_user_id) for pxls_name_id, name, pxls_user_id in merged_cur.fetchall()}

    merged_cur.execute("SELECT MAX(pxls_name_id) FROM pxls_name")
    max_name_id = merged_cur.fetchone()[0] or 0
    merged_cur.execute("SELECT MAX(pxls_user_id) FROM pxls_user")
    max_user_id = merged_cur.fetchone()[0] or 0

    mapping = {}
    user_mapping = {}

    for name, (old_name_id, old_user_id) in new_names.items():
        if name in merged_names:
            mapping[old_name_id] = merged_names[name][0]
            user_mapping[old_user_id] = merged_names[name][1]
        else:
            if old_user_id in user_mapping:
                new_user_id = user_mapping[old_user_id]
            else:
                max_user_id += 1
                new_user_id = max_user_id
                user_mapping[old_user_id] = new_user_id
                merged_cur.execute("INSERT OR IGNORE INTO pxls_user (pxls_user_id) VALUES (?)", (new_user_id,))

            max_name_id += 1
            mapping[old_name_id] = max_name_id
            merged_cur.execute(
                "INSERT INTO pxls_name (pxls_name_id, pxls_user_id, name) VALUES (?, ?, ?)",
                (max_name_id, new_user_id, name)
            )
            
    merged_conn.commit()
    new_conn.close()
    merged_conn.close()
    print("pxls_name and pxls_user mapping complete.")
    return mapping, user_mapping

def remap_and_merge_new_into_merged(db_new_path, db_merged_path, table_rules, mapping, user_mapping, record_id_offset):
    """
    Remaps data and merges it
    """
    new_conn = sqlite3.connect(db_new_path)
    merged_conn = sqlite3.connect(db_merged_path)
    new_cur = new_conn.cursor()
    merged_cur = merged_conn.cursor()

    print("Starting remapping and merge from DB_NEW...")

    for table, rules in table_rules.items():
        # Skips already managed tables
        if table in ['pxls_user', 'pxls_name']:
            print(f"Skipping table '{table}' as it has already been merged.")
            continue
        
        try:
            new_cur.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in new_cur.fetchall()]
            col_str = ", ".join(columns)
            placeholders = ", ".join(["?"] * len(columns))

            new_cur.execute(f"SELECT * FROM {table}")
            rows = new_cur.fetchall()
            if not rows:
                continue

            remapped_rows = []
            for row_tuple in rows:
                row_dict = dict(zip(columns, row_tuple))

                # Remap values based on the specific mappings provided.
                if 'record_id' in row_dict and row_dict['record_id'] is not None:
                    row_dict['record_id'] += record_id_offset
                
                if 'pxls_name_id' in row_dict and row_dict['pxls_name_id'] in mapping:
                    row_dict['pxls_name_id'] = mapping[row_dict['pxls_name_id']]

                if 'pxls_user_id' in row_dict and row_dict['pxls_user_id'] in user_mapping:
                    row_dict['pxls_user_id'] = user_mapping[row_dict['pxls_user_id']]

                remapped_rows.append(tuple(row_dict.values()))

            if remapped_rows:
                merged_cur.executemany(
                    f"INSERT OR IGNORE INTO {table} ({col_str}) VALUES ({placeholders})", remapped_rows
                )
            print(f"Merged {len(remapped_rows)} rows into {table}.")

        except Exception as e:
            print(f"Error processing table {table}: {e}")

    merged_conn.commit()
    new_conn.close()
    merged_conn.close()
    print("Remapping and merge from DB_NEW complete.")

def merge_command_usage_from_sql(merged_db_path, malformed_sql_path, new_sql_path):
    """
    Merges the 'command_usage' table by executing SQL dump files
    """
    print("\nMerging 'command_usage' table from .sql files...")
    
    # Helper function to read a file with multiple encodings
    def read_sql_file(file_path):
        try:
            # First, try the standard utf-8 encoding
            with open(file_path, 'r', encoding='utf-8') as f:
                return f.read()
        except UnicodeDecodeError:
            # If utf-8 fails, try latin-1, which is more forgiving
            print(f"  - utf-8 decoding failed for {file_path}. Retrying with 'latin-1' encoding.")
            with open(file_path, 'r', encoding='latin-1') as f:
                return f.read()
    
    if not os.path.exists(malformed_sql_path) or not os.path.exists(new_sql_path):
        print(f"  - Warning: One or both SQL files not found. Skipping.")
        return

    try:
        with sqlite3.connect(merged_db_path) as conn:
            # 1. Load data from the malformed (old) database .sql file
            print(f"  - Loading data from {malformed_sql_path}...")
            malformed_script = read_sql_file(malformed_sql_path)
            conn.executescript(malformed_script)
            print("    - Malformed DB data loaded.")

            # 2. Load data from the new database .sql file
            print(f"  - Loading data from {new_sql_path}...")
            new_script = read_sql_file(new_sql_path)
            conn.executescript(new_script)
            print("    - New DB data loaded.")
            
            conn.commit()
            print("  - 'command_usage' table merged successfully.")

    except sqlite3.Error as e:
        print(f"  - An error occurred during 'command_usage' merge from .sql: {e}")

def load_c78a_corrections(json_path):
    print(f"Loading pixel corrections from {json_path}...")
    corrections = {}
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if 'by_user' in data and isinstance(data['by_user'], dict):
                user_data_block = data['by_user']
                print(f"  - Found user data in 'by_user' key.")
                
                for username, stats in user_data_block.items():
                    if isinstance(stats, dict) and 'actions' in stats and 'undo_actions' in stats['actions']:
                        undo_actions = stats['actions']['undo_actions']
                        if isinstance(undo_actions, int):
                            correction_value = undo_actions * 2
                            corrections[username] = correction_value
            else:
                print("  - Error: Could not find the 'by_user' key in the JSON file. Cannot load corrections.")

        print(f"  - Loaded corrections for {len(corrections)} users.")
        return corrections
    except FileNotFoundError:
        print(f"  - Warning: Correction file '{json_path}' not found. Skipping correction step.")
        return {}
    except (json.JSONDecodeError, TypeError) as e:
        print(f"  - Error: Could not parse JSON from '{json_path}'. Error: {e}. Skipping correction step.")
        return {}

def get_id_to_name_map(db_path):
    """
    Creates a mapping from a user's pxls_name_id to their most recent username.
    This is used to link the JSON data (keyed by name) to the database (keyed by ID).
    Also cause Clueless doesn't have pxls' userID's, so we need to remap the freaks
    """
    print("Generating user ID-to-Name map...")
    id_map = {}
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        # This query ensures we get the most recent name for each user
        query = """
            SELECT pn.pxls_name_id, pn.name
            FROM pxls_name pn
            INNER JOIN (
                SELECT pxls_user_id, MAX(pxls_name_id) AS max_id
                FROM pxls_name GROUP BY pxls_user_id
            ) AS latest ON pn.pxls_user_id = latest.pxls_user_id AND pn.pxls_name_id = latest.max_id;
        """
        for row in conn.cursor().execute(query):
            id_map[row['pxls_name_id']] = row['name']
    print(f"  - Mapped {len(id_map)} unique users.")
    return id_map

def salvage_orphaned_stats(db_path):
    """
    Attempts to save as much existing data as possible
    """
    print("Salvaging orphaned pxls_user_stat rows...")

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        # Find the last valid record for datetime/canvas_code reference
        last_rec = cur.execute(
            "SELECT record_id, datetime, canvas_code FROM record ORDER BY record_id DESC LIMIT 1"
        ).fetchone()
        if not last_rec:
            print("No records found, cannot salvage.")
            return

        last_datetime = datetime.fromisoformat(last_rec['datetime'])
        last_canvas_code = last_rec['canvas_code']

        # Find orphaned record_ids
        orphaned_ids = [
            row['record_id'] for row in cur.execute(
                "SELECT DISTINCT record_id FROM pxls_user_stat WHERE record_id NOT IN (SELECT record_id FROM record) ORDER BY record_id"
            )
        ]
        print(f"Found {len(orphaned_ids)} orphaned record_ids.")

        # Insert new record rows for each orphaned record_id
        for i, orphan_id in enumerate(orphaned_ids, 1):
            # Guess plausible datetime (e.g., last_datetime + 15*i minutes)
            new_dt = last_datetime + timedelta(minutes=15 * i)
            cur.execute(
                "INSERT INTO record (record_id, datetime, canvas_code) VALUES (?, ?, ?)",
                (orphan_id, new_dt.isoformat(" "), last_canvas_code)
            )

        conn.commit()
        print(f"Inserted {len(orphaned_ids)} missing record rows for orphaned stats.")

def process_gimmick_canvas_stats(db_path, json_path, id_to_name_map, canvas_code_to_fill="78a"):
    """
    Refactors the gimmick canvas ('78a') stats with the final, correct logic.
    1. Interpolates 'pixels_placed' during c78a, adding to alltime_count. Canvas_count for 78a is now correctly calculated.
    2. Applies the final undo correction to the alltime_count and canvas_count for the specified record range.
    3. Applies the undo offset to canvas_count for all remaining records with canvas_code 78.
    """
    print(f"\nRefactoring stats for gimmick canvas '{canvas_code_to_fill}'...")

    gimmick_stats_map = {}
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if 'by_user' in data and isinstance(data['by_user'], dict):
                for username, stats in data['by_user'].items():
                    if isinstance(stats, dict) and 'pixels' in stats and 'actions' in stats:
                        pixels_placed = stats['pixels'].get('pixels_placed', 0)
                        undo_actions = stats['actions'].get('undo_actions', 0)
                        gimmick_stats_map[username] = {
                            "pixels_placed": pixels_placed,
                            "total_correction": pixels_placed + (undo_actions * 2),
                            "undo_correction": undo_actions * 2
                        }
        print(f"  - Loaded gimmick stats for {len(gimmick_stats_map)} users from JSON.")
        if "Falken" in gimmick_stats_map:
            gimmick_stats_map["Falkenond"] = gimmick_stats_map["Falken"]
    except Exception as e:
        print(f"  - Error loading c78a JSON: {e}. Aborting gimmick canvas processing.")
        return

    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        try:
            conn.execute("BEGIN TRANSACTION;")

            gimmick_records = cur.execute(
                "SELECT record_id FROM record WHERE canvas_code = ? ORDER BY record_id ASC", 
                (canvas_code_to_fill,)
            ).fetchall()
            if not gimmick_records:
                print(f"  - No records found for canvas_code '{canvas_code_to_fill}'. Skipping."); return

            num_intervals = len(gimmick_records)
            pre_gimmick_rec_id = gimmick_records[0]['record_id'] - 1

            pre_gimmick_stats = {
                row['pxls_name_id']: dict(row)
                for row in cur.execute(
                    "SELECT pxls_name_id, alltime_count, canvas_count FROM pxls_user_stat WHERE record_id = ?", 
                    (pre_gimmick_rec_id,)
                )
            }

            stats_to_insert = []
            for i, rec in enumerate(gimmick_records):
                for uid, username in id_to_name_map.items():
                    if username in gimmick_stats_map and uid in pre_gimmick_stats:
                        pixels_placed = gimmick_stats_map[username]["pixels_placed"]
                        interpolated_pixels = int((pixels_placed / num_intervals) * (i + 1))
                        base_alltime = pre_gimmick_stats[uid].get('alltime_count', 0) or 0
                        
                        # **THE FIX**: The canvas_count for the gimmick canvas is JUST the interpolated pixels
                        # for that canvas. It does NOT include the count from the previous canvas.
                        stats_to_insert.append(
                            (rec['record_id'], uid, base_alltime + interpolated_pixels, interpolated_pixels)
                        )

            print(f"  - Inserting/updating {len(stats_to_insert)} stat entries for the gimmick canvas.")
            if stats_to_insert:
                cur.executemany(
                    "INSERT OR REPLACE INTO pxls_user_stat (record_id, pxls_name_id, alltime_count, canvas_count) VALUES (?, ?, ?, ?)",
                    stats_to_insert
                )

            # Part 2: Apply the final correction to the specified range
            start_correction_id, end_correction_id = 94725, 94868
            print(f"  - Applying final undo corrections to alltime_count and canvas_count for records {start_correction_id} to {end_correction_id}...")
            updates = []
            for uid, username in id_to_name_map.items():
                if username in gimmick_stats_map:
                    correction = gimmick_stats_map[username]["total_correction"]
                    undo_correction = gimmick_stats_map[username]["undo_correction"]
                    updates.append((correction, undo_correction, uid))
            if updates:
                cur.executemany(f"""
                    UPDATE pxls_user_stat
                    SET alltime_count = alltime_count + ?, canvas_count = canvas_count + ?
                    WHERE pxls_name_id = ? AND record_id BETWEEN {start_correction_id} AND {end_correction_id}
                """, updates)
            print(f"  - Updated {cur.rowcount} stat entries across the specified range.")

            # Part 3: Apply undo offset to canvas_count for all remaining records
            print("  - Applying undo offset to canvas_count for all remaining records with canvas_code 78...")
            for uid, username in id_to_name_map.items():
                if username in gimmick_stats_map:
                    undo_correction = gimmick_stats_map[username]["undo_correction"]
                    cur.execute(f"""
                        UPDATE pxls_user_stat
                        SET canvas_count = canvas_count + ?
                        WHERE pxls_name_id = ?
                        AND record_id IN (
                            SELECT record_id FROM record
                            WHERE canvas_code = '78' AND record_id > ?
                        )
                    """, (undo_correction, uid, end_correction_id))

            conn.commit()
            print("  - Gimmick canvas refactor complete.")

        except Exception as e:
            print(f"An error occurred during gimmick canvas processing: {e}"); conn.rollback()

def fill_complex_gap_with_reset(db_merged_path, db_new_path, mapping, reset_datetime_str, old_canvas_code=78, new_canvas_code=79):
    """
    Fills the complex data gap between the now-fully-corrected C78 data and C79
    with hourly records, using a robust, stateless calculation.
    """
    print("\nFilling main complex gap between C78 and C79...")

    merged_conn = sqlite3.connect(db_merged_path)
    merged_conn.row_factory = sqlite3.Row
    merged_cur = merged_conn.cursor()
    new_conn = sqlite3.connect(db_new_path)
    new_conn.row_factory = sqlite3.Row
    new_cur = new_conn.cursor()

    try:
        last_stat_rec_id_row = merged_cur.execute("SELECT record_id FROM pxls_user_stat ORDER BY record_id DESC LIMIT 1").fetchone()
        if not last_stat_rec_id_row:
            print("Could not find any user stats. Aborting gap fill."); return
        last_rec = merged_cur.execute("SELECT record_id, datetime FROM record WHERE record_id = ?", (last_stat_rec_id_row['record_id'],)).fetchone()
        
        first_rec_new_db = new_cur.execute("SELECT record_id, datetime FROM record ORDER BY datetime ASC LIMIT 1").fetchone()
        if not last_rec or not first_rec_new_db:
            print("Could not find boundary records."); return

        print(f"  - Starting gap fill from last valid data point at record_id: {last_rec['record_id']}")

        start_datetime = datetime.fromisoformat(last_rec['datetime'])
        reset_datetime = datetime.fromisoformat(reset_datetime_str)
        end_datetime = datetime.fromisoformat(first_rec_new_db['datetime'])

        q = "SELECT pxls_name_id, alltime_count, canvas_count FROM pxls_user_stat WHERE record_id = ?"
        stats_A = {row['pxls_name_id']: dict(row) for row in merged_cur.execute(q, (last_rec['record_id'],))}
        stats_D_raw = {row['pxls_name_id']: dict(row) for row in new_cur.execute(q, (first_rec_new_db['record_id'],))}
        stats_D = {mapping.get(old_id): data for old_id, data in stats_D_raw.items() if mapping.get(old_id)}

        all_user_ids = sorted(list(set(stats_A.keys()) | set(stats_D.keys())))
        
        stats_B, stats_C = {}, {}
        for uid in all_user_ids:
            stats_at_d = stats_D.get(uid)
            stats_at_a = stats_A.get(uid, {'alltime_count': 0, 'canvas_count': 0})
            
            # Robust NoneType Handling
            alltime_val = (stats_at_d or stats_at_a).get('alltime_count', 0)
            canvas_val = (stats_at_d or {}).get('canvas_count', 0)
            if alltime_val is None:
                alltime_val = 0
            if canvas_val is None:
                canvas_val = 0
            
            final_alltime_c78 = alltime_val - canvas_val
            stats_B[uid] = {'alltime_count': final_alltime_c78, 'canvas_count': final_alltime_c78}
            stats_C[uid] = {'alltime_count': final_alltime_c78, 'canvas_count': 0}

        next_record_id = last_rec['record_id'] + 1
        all_records, all_stats = [], []

        # Part 1: C78 Gap
        hours_part1 = int((reset_datetime - start_datetime).total_seconds() / 3600)
        if hours_part1 > 0:
            print(f"  - Generating {hours_part1} hourly records for C{old_canvas_code}...")
            for i in range(hours_part1):
                hour_num, rec_id = i + 1, next_record_id + i
                rec_date = (start_datetime + timedelta(hours=hour_num))
                all_records.append((rec_id, rec_date.isoformat(" "), old_canvas_code))
                for uid in all_user_ids:
                    s_ac = (stats_A.get(uid) or {}).get('alltime_count', 0) or 0
                    s_cc = (stats_A.get(uid) or {}).get('canvas_count', 0) or 0
                    e_ac = (stats_B.get(uid) or {}).get('alltime_count', s_ac) or s_ac
                    diff_ac = e_ac - s_ac
                    diff_cc = diff_ac
                    current_ac = int(s_ac + (diff_ac * hour_num / hours_part1))
                    current_cc = int(s_cc + (diff_cc * hour_num / hours_part1))
                    all_stats.append((rec_id, uid, current_ac, current_cc))
        
        # Part 2: C79 Gap
        hours_part2 = int((end_datetime - reset_datetime).total_seconds() / 3600)
        start_id_part2 = next_record_id + hours_part1
        if hours_part2 > 0:
            print(f"  - Generating {hours_part2} hourly records for C{new_canvas_code}...")
            for i in range(hours_part2):
                hour_num, rec_id = i + 1, start_id_part2 + i
                rec_date = (reset_datetime + timedelta(hours=hour_num))
                all_records.append((rec_id, rec_date.isoformat(" "), new_canvas_code))
                for uid in all_user_ids:
                    s_ac = (stats_C.get(uid) or {}).get('alltime_count', 0) or 0
                    s_cc = (stats_C.get(uid) or {}).get('canvas_count', 0) or 0
                    e_stats = stats_D.get(uid)
                    e_ac = (e_stats or {}).get('alltime_count', s_ac) or s_ac
                    e_cc = (e_stats or {}).get('canvas_count', s_cc) or s_cc
                    diff_ac, diff_cc = e_ac - s_ac, e_cc - s_cc
                    
                    current_ac = int(s_ac + (diff_ac * hour_num / hours_part2))
                    current_cc = int(s_cc + (diff_cc * hour_num / hours_part2))
                    all_stats.append((rec_id, uid, current_ac, current_cc))

        if all_records:
            merged_cur.executemany("INSERT INTO record (record_id, datetime, canvas_code) VALUES (?, ?, ?)", all_records)
            merged_cur.executemany("INSERT INTO pxls_user_stat (record_id, pxls_name_id, alltime_count, canvas_count) VALUES (?, ?, ?, ?)", all_stats)
            merged_conn.commit()
            print(f"Successfully inserted {len(all_records)} hourly records.")

    except Exception as e:
        print(f"An error occurred during main gap filling: {e}"); merged_conn.rollback()
    finally:
        merged_conn.close(); new_conn.close()
            
# --- Main Execution Logic ---
if __name__ == "__main__":
    if os.path.exists(DB_MERGED):
        os.remove(DB_MERGED)
        print(f"Removed existing '{DB_MERGED}'.")

    # Step 1: Recover data from the malformed database.
    recovery_success = recover_sql_data() and load_recovered_sql(DB_MERGED, RECOVERED_SQL)
    if not recovery_success:
        print("Recovery from .sql dump failed. Using fallback.")
        copy_schema(DB_NEW, DB_MERGED)
        extract_and_copy_data(DB_MALFORMED, DB_MERGED, TABLE_RULES)
        with sqlite3.connect(DB_MERGED) as conn:    
            conn.execute("DELETE FROM record")
            conn.commit()
            print("Removed all records from 'record' table in merged DB.")
            load_table_sql("record.sql", DB_MERGED) # Your special record recovery

    merge_command_usage_from_sql(
        DB_MERGED, 
        "malformed_command_usage.sql", 
        "new_command_usage.sql"
    )

    salvage_orphaned_stats(DB_MERGED)

    # Step 2: Build mappings
    mapping, user_mapping = build_pxls_name_mapping(DB_NEW, DB_MERGED)

    # Step 3: Create ID-to-Name map
    id_to_name = get_id_to_name_map(DB_MERGED)
    
    # Step 4: Process the c78a gimmick canvas stats and apply all its corrections
    process_gimmick_canvas_stats(DB_MERGED, "78a.json", id_to_name)

    # Step 5: Fill the main gap between the now-fully-corrected data and C79
    fill_complex_gap_with_reset(
        DB_MERGED, DB_NEW, mapping, 
        reset_datetime_str="2024-04-07 17:00:00"
    )

    # Step 6 & 7: Calculate offset and merge the new DB
    record_id_offset = 0
    with sqlite3.connect(DB_MERGED) as conn_merged, sqlite3.connect(DB_NEW) as conn_new:
        merged_max_record_id = conn_merged.execute("SELECT MAX(record_id) FROM record").fetchone()[0] or 0
        new_min_record_id = conn_new.execute("SELECT MIN(record_id) FROM record").fetchone()[0] or 0
        if new_min_record_id is not None:
             record_id_offset = merged_max_record_id - new_min_record_id + 1
    print(f"\nCalculated final record_id offset: {record_id_offset}")

    remap_and_merge_new_into_merged(DB_NEW, DB_MERGED, TABLE_RULES, mapping, user_mapping, record_id_offset)
    
    print("\nDatabase merge process finished.")