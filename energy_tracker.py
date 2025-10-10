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

MYSQL_CONF = "/etc/mysql/my.cnf"

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

    def _report(self, options_group):
        """Repoer options"""
        options_group.add_argument("-r", help="report on collected data", action="store_true")
        options_group.add_argument("--last-ten", help="last ten results", action="store_true")

    def _general(self, options_group):
        """General inputs"""
        options_group.add_argument("-d", help="debug logging", action="store_true")
        options_group.add_argument("-reading", help="current reading in kWh", type=float)

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
        query = "SELECT reading FROM Readings ORDER BY timestamp DESC LIMIT 1;"
        self._run_query(query)
        if self.results_dict:
            return self.results_dict[1]["reading"]
        return 0

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

        query = f"INSERT INTO Readings (timestamp, reading, changes) VALUES (%s, %s, %s);"
        data = (timestamp, reading, changes)
        self.logger.info("Saving data")

        cursor = None
        
        try:
            cursor = self.cnx.cursor()
            cursor.execute(query, data)
            self.cnx.commit()
            self.logger.info("Data stored successfully")
        except (ProgrammingError, OperationalError) as e:
            self.logger.error("Unable to store data: %s", str(e))
        except Exception as e:
            self.logger.error("Error %s storing data: %s", type(e).__name__, str(e))
        finally:
            if cursor:
                cursor.close()

    def disconnect(self):
        """Close database connection"""
        if self.connected:
            self.cnx.close()

def run_report():
    """Run a report on the collected data"""
    logger.info("Not implemented yet")
    pass

def add_data(db_h):
    """Add data to the database"""
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
        run_report()
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
    
    logger.info("Script started")
        
    main()

    logger.info("Script finished successfully")