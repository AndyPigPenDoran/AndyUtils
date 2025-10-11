import sys
import logging
import argparse
import mysql.connector
import getpass

from mysql.connector.errors import ProgrammingError, OperationalError
from datetime import datetime, timezone

# Constants
DEFAULT_CONNECT_TIMEOUT = 10
DEFAULT_DATABSE = "Electricity"
DEFAULT_READINGS_TABLE = "Readings"
DEFAULT_EV_TABLE = "EV"
DEFAULT_EV_CAPACITY = 58  # kWh

MYSQL_CONF = "/etc/mysql/my.cnf"

STARS = "*" * 132

args = None

def pad_string(input_string, pad_length, pad_char=" ") -> str:
    """Pad a string to the desiered size"""
    if len(input_string) >= pad_length:
        return input_string
    padding = pad_char * (pad_length - len(input_string))
    return f"{input_string}{padding}"

def get_key_from_config(config_file, key) -> str:
    """Read a file looking for the key - format of ... socket = abc"""
    key_text = f"{key} ="
    file_contents = []

    try:
        with open(config_file, "r") as f:
            file_contents = f.readlines()
    except Exception:
        return None
    
    if not file_contents:
        return None
    
    for line in file_contents:
        if line[:len(key_text)] == key_text:
            return line.split("=")[1].strip()
        
    return None

def get_timestamp() -> int:
    """Get the current timestamp (seconds since 1-Jan-1970)"""
    dt_now = datetime.now(timezone.utc)
    return int(dt_now.timestamp())

def convert_timestamp(timestamp) -> datetime:
    """Turn timestamp into datetime"""
    return datetime.fromtimestamp(timestamp)

def format_time_long(dt) -> str:
    """Return as 21-Jul-2025 10:45am"""
    return dt.strftime("%d-%b-%Y %I:%M%p")

class HandleArgs:
    """Handle command line arguments"""
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.args = None

    def _mysql(self, options_group):
        """MySQL Arguments"""
        options_group.add_argument("-user", help="MySQL user", type=str)
        options_group.add_argument("-pwd", help="MySQL password", type=str)
        options_group.add_argument(
            "-host", help="MySQL host (default is Localhost)", type=str, default="127.0.0.1"
        )
        options_group.add_argument("-port", help="MySQL port", type=int, default=3306)
        options_group.add_argument(
            "-database", help="database to use", type=str, default=DEFAULT_DATABSE
        )
        options_group.add_argument(
            "--connect-timeout", help="timeout (in seconds) for MySQL connection", type=int,
            default=DEFAULT_CONNECT_TIMEOUT
        )
        options_group.add_argument("--use-socket", help="use Unix socket", action="store_true")
        options_group.add_argument(
            "--readings-table", help="table for readings", type=str, default=DEFAULT_READINGS_TABLE
        )
        options_group.add_argument(
            "--ev-table", help="table for EV data", type=str, default=DEFAULT_EV_TABLE
        )

    def _report(self, options_group):
        """Repoer options"""
        options_group.add_argument("-r", help="report on collected data", action="store_true")
        options_group.add_argument("--last-ten", help="last ten results", action="store_true")
        options_group.add_argument(
            "--ev-capacity", help="capacity of EV battery in kWh", type=float,
            default=DEFAULT_EV_CAPACITY
        )

    def _general(self, options_group):
        """General inputs"""
        options_group.add_argument("-d", help="debug logging", action="store_true")
        options_group.add_argument("-reading", help="current reading in kWh", type=float)
        options_group.add_argument(
            "--ev-charge", help="record EV charge in kWh", action="store_true"
        )
        options_group.add_argument("--start-level", help="EV start level in kWh", type=float)
        options_group.add_argument("--end-level", help="EV end level in kWh", type=float)
        options_group.add_argument(
            "-test", help="test mode - do not store data", action="store_true"
        )

    def parse_arguments(self):
        """Build argument groups and process the inputs"""
        g_sql = self.parser.add_argument_group("MySQL Options")
        self._mysql(g_sql)

        g_report = self.parser.add_argument_group("Report Options")
        self._report(g_report)

        g_general = self.parser.add_argument_group("General Options")
        self._general(g_general)

        self.args = self.parser.parse_args()   

class CustomFormatter(logging.Formatter):
    """To add colour to what we do"""

    grey = "\x1b[38;20m"
    blue = "\x1b[36;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"

    format = "%(levelname)s: %(message)s"

    FORMATS = {
        logging.DEBUG: blue + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record) 

class DatabaseHandler:
    def __init__(self, logger, args):
        self.logger = logger
        self.args = args
        self.connection_dict = self._connection_dict()
        self.cnx = None
        self.connected = None
        self.results_dict = {}

    def _connection_dict(self):
        """Build a connection dictionary from the parsed arguments"""
        return {"host": "127.0.0.1"}
    

    def _make_dict_from_cursor(self, cursor):
        """Make a dictionary from the cursor results"""
        results = {}
        columns = [col for col in cursor.column_names]
        rows = cursor.fetchall()
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col] = row[i]
            results[len(results) + 1] = row_dict
        return results

    def _run_query(self, query):
        """Run a SQL query"""
        # self.logger.debug(f"Query: {query}")
        cursor = None
        self.results_dict = {}

        try:
            cursor = self.cnx.cursor()
            cursor.execute(query)
            #r = self.dbc.store_result()
            self.results_dict = self._make_dict_from_cursor(cursor)
        except (ProgrammingError, OperationalError) as e:
            self.logger.error(
                f"Unable to run the query: {str(e)}"
            )
            self.results_dict = {}
        finally:
            if cursor:
                cursor.close()
    
    def _get_last_reading(self) -> int:
        """Get the last reading from the database"""
        query = f"SELECT reading FROM {args.readings_table} ORDER BY timestamp DESC LIMIT 1;"
        self._run_query(query)
        if self.results_dict:
            return self.results_dict[1]["reading"]
        return 0

    def _write_data(self, query, data, what="Meter reading"):
        """Write data to the database"""
        self.logger.info(f"Saving data for: {what}")
        cursor = None
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query, data)
            self.cnx.commit()
        except (ProgrammingError, OperationalError) as e:
            self.logger.error(f"Unable to store data for {what}. Error: {str(e)}")
        except Exception as e:
            self.logger.error(
                f"Error {type(e).__name__} storing data for {what}. Error: {str(e)}"
            )
        finally:
            if cursor:
                cursor.close()

    def connect(self):
        """Connect to the database"""
        # Do we have a password??
        if not self.connection_dict.get("password", None):
            user = self.connection_dict.get("user", None)
            if not user:
                user = getpass.getuser()            
            pwd = getpass.getpass(f"Password for user {user}: ")
            if pwd:
                self.connection_dict["password"] = pwd
            else:
                self.logger.warning(
                    "No password was provided, Database connection cannot be established"
                )
                return
        try:
            self.cnx = mysql.connector.connect(**self.connection_dict)
            self.connected = True
            self.logger.debug("Connected to MySQL")
        except (ProgrammingError, OperationalError) as e:
            self.logger.error(
                "Connection failed, make sure MySQL is installed on the specified host and that " \
                f"the connection information is correct. Error: {str(e)}"
            )
        except Exception as e:
            error_type = type(e).__name__
            error_str = str(e)
            self.logger.error(
                f"Error {error_type} connecting to MySQL: {error_str}"
            )

    def get_connection_args(self):
        """Connection string for database"""
        update_dict = {}
        if self.args.host:
            update_dict["host"] = self.args.host
        if self.args.port and not self.args.use_socket:
            update_dict["port"] = self.args.port
        if self.args.user:
            update_dict["user"] = self.args.user
        if self.args.pwd:
            update_dict["password"] = self.args.pwd
        if self.args.connect_timeout:
            update_dict["connect_timeout"] = self.args.connect_timeout
        if self.args.database:
            update_dict["database"] = self.args.database
        if self.args.use_socket:
            socket_file = get_key_from_config(MYSQL_CONF, "socket")
            if socket_file:
                update_dict["unix_socket"] = socket_file

        if update_dict:
            self.connection_dict.update(update_dict)

        # Log this info
        _msg = "\n\nMySQL Connection info:\n"
        for k, v in self.connection_dict.items():
            if k == "password":
                v = "**********"
            _msg += f"\n  {pad_string(k, 15)}: {v}"
        self.logger.debug(f"{_msg}\n")

    def store_reading(self, reading):
        """Store a reading in the database"""
        # Last reading?
        last_reading = self._get_last_reading()
        changes = reading - last_reading
        timestamp = get_timestamp()

        if changes < 0:
            self.logger.warning(
                f"New reading {reading} is less than last reading {last_reading}, no update made"
            )
            return

        query = (
            f"INSERT INTO {args.readings_table} (timestamp, reading, changes) "
            "VALUES (%s, %s, %s);"
        )
        data = (timestamp, reading, changes)
        self._write_data(query, data, what="Meter reading")

    def store_ev_reading(self, start_level, end_level):
        """Store an EV charge event - probably change to share code with store_reading"""
        timestamp = get_timestamp()

        query = (
            f"INSERT INTO {args.ev_table} (timestamp, start, end) "
            "VALUES (%s, %s, %s);"
        )
        data = (timestamp, start_level, end_level)
        self._write_data(query, data, what="EV charge")

    def get_reading_summary(self):
        """Get a summary of the readings"""
        query = (
            "SELECT FLOOR(SUM(changes)) AS used, "
            "FLOOR(MIN(timestamp)) AS start_timestamp, FLOOR(MAX(timestamp)) AS end_timestamp "
            f"FROM {args.readings_table};"
        )
        self._run_query(query)
        return self.results_dict[1] if self.results_dict else {}

    def get_ev_summary(self):
        """Get a summary of the EV charges"""
        query = (
            "SELECT COUNT(*) AS charges, "
            "FLOOR(SUM(end - start)) AS total_charged, "
            "FLOOR(AVG(end - start)) AS avg_charge, "
            "FLOOR(MIN(timestamp)) AS start_timestamp, "
            "FLOOR(MAX(timestamp)) AS end_timestamp "
            f"FROM {args.ev_table};"
        )
        self._run_query(query)
        return self.results_dict[1] if self.results_dict else {}

    def disconnect(self):
        """Close database connection"""
        if self.connected:
            self.cnx.close()

def report_summary(summary):
    """Report the summary of readings"""
    start_ts = summary.get("start_timestamp", None)
    end_ts = summary.get("end_timestamp", None)
    if start_ts is None or end_ts is None:
        print("No data to report")
        return

    start_dt = convert_timestamp(summary["start_timestamp"])
    end_dt = convert_timestamp(summary["end_timestamp"])
    used = int(summary["used"])

    print("\nSummary of readings")
    print("-------------------")
    print(f"  Start time : {format_time_long(start_dt)}")
    print(f"  End time   : {format_time_long(end_dt)}")
    print(f"  Total used : {used} kWh\n")

    if start_ts == end_ts:
        print("Only one reading in the database, no further report possible\n")
        return

    total_hours = (end_ts - start_ts) / 3600
    avg_per_day = used / (total_hours / 24)
    avg_per_month = avg_per_day * 30
    avg_per_year = avg_per_day * 365
    print(f"  Total time        : {total_hours:.1f} hours")
    print(f"  Average per day   : {avg_per_day:.1f} kWh")
    print(f"  Average per month : {avg_per_month:.1f} kWh")
    print(f"  Average per year  : {avg_per_year:.1f} kWh\n")

def get_kwh_from_percent(percent, capacity):    
    """Get kWh from a percentage of the capacity"""
    return (percent / 100) * capacity

def report_ev_summary(ev_summary):
    """Report the summary of EV charges"""
    charges = ev_summary.get("charges", 0)
    total_charged = get_kwh_from_percent(ev_summary.get("total_charged", 0), args.ev_capacity)
    avg_charge = get_kwh_from_percent(ev_summary.get("avg_charge", 0), args.ev_capacity)

    start_ts = ev_summary.get("start_timestamp", None)
    end_ts = ev_summary.get("end_timestamp", None)
    if start_ts is None or end_ts is None:
        print("No data to report")
        return

    start_dt = convert_timestamp(ev_summary["start_timestamp"])
    end_dt = convert_timestamp(ev_summary["end_timestamp"])

    print("\nSummary of EV charges")
    print("---------------------")
    print(f"  Start time        : {format_time_long(start_dt)}")
    print(f"  End time          : {format_time_long(end_dt)}")
    print(f"  Number of charges : {charges}")
    print(f"  Total charged     : {total_charged} kWh")
    print(f"  Average charge    : {avg_charge} kWh")
    print(f"  EV Capacity       : {args.ev_capacity} kWh\n")

    if start_ts == end_ts:
        print("Only one reading in the database, no further report possible\n")
        return

    total_hours = (end_ts - start_ts) / 3600
    avg_per_day = int(total_charged) / (total_hours / 24)
    avg_per_month = avg_per_day * 30
    avg_per_year = avg_per_day * 365
    print(f"  Total time        : {total_hours:.1f} hours")
    print(f"  Average per day   : {avg_per_day:.1f} kWh")
    print(f"  Average per month : {avg_per_month:.1f} kWh")
    print(f"  Average per year  : {avg_per_year:.1f} kWh\n")        

def run_report(db_h):
    """Run a report on the collected data"""
    summary = db_h.get_reading_summary()
    # print(summary)

    if not summary or summary.get("start_timestamp", None) is None:
        print("No data to report")
        return

    report_summary(summary)

    ev_summary = db_h.get_ev_summary()
    if not ev_summary or ev_summary.get("charges", None) is None:    
        print("No EV data to report")
        return

    report_ev_summary(ev_summary)

def add_data(db_h):
    """Add data to the database"""
    if args.ev_charge:
        if args.start_level is None or args.end_level is None:
            logger.error("Both --start-level and --end-level must be provided to log EV charge")
            return
        if args.start_level < 0 or args.end_level < 0:
            logger.error("Start and end levels must be positive values")
            return
        if args.start_level >= args.ev_capacity or args.end_level > args.ev_capacity:
            logger.error(
                f"Start and end levels must be less than the EV capacity of {args.ev_capacity} kWh"
            )
            return
        if args.end_level <= args.start_level:
            logger.error("End level must be greater than start level")
            return

        db_h.store_ev_reading(args.start_level, args.end_level)
        return

    if not args.reading:
        logger.error("A reading must be provided with the -reading argument to add data")
        return
    db_h.store_reading(args.reading)

def main():
    """Main function to parse arguments and execute the script logic."""
    db_h = DatabaseHandler(logger, args)
    db_h.get_connection_args()
    db_h.connect()
    
    if not db_h.connected:
        logger.error("Database connection could not be established, exiting")
        return
    if args.r:
        run_report(db_h)
    else:
        add_data(db_h)

    db_h.disconnect()


if __name__ == "__main__":
    input_h = HandleArgs()
    input_h.parse_arguments()
    args = input_h.args
    
    # Configure logging
    logger = logging.Logger(__name__)
    stdout_h = logging.StreamHandler(sys.stdout)
    log_level = logging.DEBUG if args.d else logging.INFO
    stdout_h.setLevel(log_level)
    stdout_h.setFormatter(CustomFormatter())
    logger.addHandler(stdout_h)
    
    print(
        f"\n{STARS}\nEnergy Tracker - track electricity usage and EV charging\n{STARS}"
    )
        
    main()


    print("")