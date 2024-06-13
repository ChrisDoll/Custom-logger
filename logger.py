import os
import sys
import datetime
from pathlib import Path
import time
import json
import atexit
import smtplib
import sqlalchemy
from sqlalchemy import text, create_engine, Column, Integer, String, DateTime, TIMESTAMP, BigInteger, select, func
from sqlalchemy.orm import sessionmaker, Mapped, mapped_column, relationship, DeclarativeBase
import pandas as pd

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.utils import formataddr

from dotenv import load_dotenv
load_dotenv()

import logging
from logging.handlers import TimedRotatingFileHandler

db_path = os.environ.get('SCRAPE_DB_PATH', 'default.db')

class Singleton:
    _instances = {}
    
    def __new__(cls, *args, **kwargs):
        if cls not in cls._instances:
            instance = super().__new__(cls)
            cls._instances[cls] = instance
        return cls._instances[cls]

# Creating an ORM for log entries:
class Base(DeclarativeBase):
    pass
class LogEntry(Base):
    __tablename__ = 'logs'
    log_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    instance_id : Mapped[int] = mapped_column(BigInteger)
    time : Mapped[datetime.datetime] = mapped_column(TIMESTAMP(timezone=False))
    area_name : Mapped[str]
    level : Mapped[str]
    message : Mapped[str]

def get_logger(name):
    """
    This function sets up a base python logger with multiple handlers for customized output.
    It also uploads logs to the SQL database.

    Effectively it works by adding a custom layer onto the logging module, allowing for customized logging.
    Currently this is not used for any special types, just generating logging reports at exit.

    The logger will have the following handlers:
    - Console handler: Outputs logs to the console with INFO level
    - Warning file handler: Outputs logs to a file with WARNING level, new file should be created every 24 hours
    - Error file handler: Outputs logs to a file with ERROR level, new file should be created every 24 hours
    - Critical file handler: Outputs logs to a file with CRITICAL level, new file should be created every 24 hours
    - Full file handler: Outputs logs to a file with DEBUG level, new file should be created every 24 hours
    
    """

    #### Custom log level methods ####
    STATUS = 25
    logging.addLevelName(STATUS, "STATUS")
    def status(self, message, *args, **kwargs):
        if self.isEnabledFor(STATUS):
            self._log(STATUS, message, args, **kwargs)
    logging.Logger.status = status
    
        
    # Initialize the logger
    logger = logging.getLogger(name)
    if not logger.handlers:

        #### Logger setup ####
        # Create a custom logger
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)  # Set the default log level
        print_format = logging.Formatter("%(levelname)s - %(name)s - %(message)s")
        full_format = logging.Formatter("%(asctime)s :-: %(name)s :-: %(levelname)s :-: %(message)s")


        #### Handlers ####
        # Console handler for printing to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(print_format)

        # Sql handler for logging to the database
        sql_handler = CustomSqlHandler()
        sql_handler.setLevel(logging.INFO)
        sql_handler.setFormatter(full_format)


        #### File handlers for writing to files
        # Checking the logs map exsists, else create it
        if not Path("logs").exists():
            Path("logs").mkdir(parents=True, exist_ok=True)
        
        warning_file_handler = TimedRotatingFileHandler("logs/warning.log", when='midnight', interval=1, backupCount=7) 
        warning_file_handler.setLevel(logging.WARNING)
        warning_file_handler.setFormatter(full_format)

        error_file_handler = TimedRotatingFileHandler("logs/error.log", when='midnight', interval=1, backupCount=7) 
        error_file_handler.setLevel(logging.ERROR)
        error_file_handler.setFormatter(full_format)

        critical_file_handler = TimedRotatingFileHandler("logs/critical.log", when='midnight', interval=1, backupCount=7) 
        critical_file_handler.setLevel(logging.CRITICAL)
        critical_file_handler.setFormatter(full_format)

        full_file_handler = TimedRotatingFileHandler("logs/all_logs.log", when='midnight', interval=1, backupCount=7) 
        full_file_handler.setLevel(logging.DEBUG)
        full_file_handler.setFormatter(full_format)

        # The collect handler is adapted to take over the tasks of the wz_etl
        custom_handler = Custom_logger()
        custom_handler.setLevel(logging.DEBUG)

        # Finally, we combine these handlers into the logger
        logger.addHandler(console_handler)
        logger.addHandler(sql_handler)
        logger.addHandler(warning_file_handler)
        logger.addHandler(error_file_handler)
        logger.addHandler(critical_file_handler)
        logger.addHandler(full_file_handler)
        logger.addHandler(custom_handler)
    # And return the finished logger
    return logger


# ################################################################################################################
# Custom Logic
# ################################################################################################################

class SQL_mini_wizard:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            engine = cls.create_engine(type='sqlite')
            if engine.type == 'sqlite':
                cls.create_table(engine)
            session_maker = sessionmaker(bind=engine)
            etl_id = cls.get_etl_id(session_maker)
            cls._instance = {
                'engine': engine,
                'session_maker': session_maker,
                'etl_id': etl_id
            }
        return cls._instance
    @staticmethod
    def create_engine(type='postgresql'):
        try:
            if type == 'sqlite':
                sqlite3.connect("logs.db")
                engine = sqlalchemy.create_engine(
                    f"sqlite:///{"logs.db"}", 
                    future=True,
                    connect_args={'detect_types': sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES}
                )
                engine.type = 'sqlite'
                return engine
            else:
                engine = sqlalchemy.create_engine(
                    f"postgresql://"
                    f"{os.environ.get('SCRAPE_DB_USER')}:"
                    f"{os.environ.get('SCRAPE_DB_PASSWORD')}@"
                    f"{os.environ.get('SCRAPE_DB_SERVER')}:"
                    f"{os.environ.get('SCRAPE_DB_PORT')}/"
                    f"{os.environ.get('SCRAPE_DB_DATABASE')}",
                    future=True
                )
                engine.type = 'postgresql'
            return engine
        except Exception as e:
            raise Exception(f'failed to establish logging connection to db with error: {e}')
    
    @staticmethod
    def get_etl_id(session_maker):
        """
        This function gets a new etl_id from the database
        """
        try:
            with session_maker() as session:
                etl_id = session.execute(
                    select(LogEntry.instance_id)
                    .order_by(LogEntry.instance_id.desc())
                ).fetchone()
                if etl_id is None:
                    print("No etl_id found")
                    return 1
                return etl_id[0] + 1
        except Exception as e:
            print(f"An error occurred: {e}")
            return 1

    @staticmethod
    def create_table(engine):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            instance_id BIGINT,
            time TIMESTAMP,
            area_name TEXT,
            level TEXT,
            message TEXT
        )
        """
        with engine.connect() as conn:
            conn.execute(text(create_table_query))

    @staticmethod
    def _delete_table():
        """
        This method deletes the table in the database.
        """
        engine = create_engine("sqlite:///logs.db")
        with engine.connect() as conn:
            conn.execute("DROP TABLE logs")
            conn.commit()
        print("Table deleted")


# Creating a handler for SQL logging
class CustomSqlHandler(logging.Handler):
    def __init__(self, etl_id = 1):
        super().__init__()
        wizard = SQL_mini_wizard.get_instance()
        self.etl_id = wizard['etl_id']
        self.session_maker = wizard['session_maker']
        self.engine = wizard['engine']

    # Emit is called whenever a log is made
    def emit(self, record):
        with self.session_maker() as session:
            log_entry = LogEntry(
                instance_id=self.etl_id,
                time=datetime.datetime.now(),
                area_name=f"{record.name}",
                level=record.levelname,
                message=record.getMessage()
            )
            session.add(log_entry)
            session.commit()
            session.close()
        #print(f"Logged {record.levelname} message from {record.name}")
        

# ################################################################################################################
# ### Setting up the custom logging handler logic
# ################################################################################################################

# Custom logger functionality

class Custom_logger(logging.Handler, Singleton):
    def __init__(self):
        if hasattr(self, 'initialized'):
            return

        super().__init__()
        self.reported_levels = ['WARNING', 'ERROR', 'EXCEPTION', 'CRITICAL']
        self.run_start = datetime.datetime.now()
        wizard = SQL_mini_wizard.get_instance()
        self.etl_id = wizard['etl_id']
        self.session_maker = wizard['session_maker']
        self.engine = wizard['engine']
        atexit.register(self.finalize_logs)
        self.initialized = True

    def emit(self, record):
        try:
            if "Status" == record.levelname:
                self.status_message(record)

        except Exception as e:
            print(f"Error in logging: {e}")

    def status_message(self, record):
        # Might want to make this split the varius logs into categories
        # Or alternatively, each time a status is logged, start a new area with the status as the area name
        pass

    def finalize_logs(self):
        print("Finalizing logs")
        self.run_end = datetime.datetime.now()
        self.generate_reports()

    def generate_reports(self):

        try:
            self._generate_runtime_report() 
            self._generate_error_report()
        except ValueError as e:
            print(f"Error generating reports: {e}")
            pass
        
        # Send mail with reports or something

        # Store self.report at the top of the json file, removing everything after 300 lines
        if not hasattr(self, 'report'):
            return

        with open('report.json', 'w') as f:
            try:
                lines = f.readlines()
                if lines:
                    lines = lines[:300]
                data = [json.loads(line) for line in lines]
                data.insert(0, self.report)
                json.dump(data, f, indent=4)
            except Exception as e:
                print(f"Error writing report: {e}")
                json.dump(self.report, f, indent=4)    

    def _generate_runtime_report(self):
        """
        Generate a runtime report for the application, currently based on the logger names.

        Each time a status log is encountered, it starts a new sub-entry under the current area.
        """
        report = {}

        with self.engine.connect() as conn:
            cursor = conn.execute(text(f"SELECT message, area_name, time, level FROM logs WHERE instance_id = '{self.etl_id}' ORDER BY time"))
            logs = cursor.fetchall()
            if len(logs) == 0:
                return
            
            # Overall runtime
            start = logs[0][2]
            end = logs[-1][2]
            overall_runtime = (end - start).total_seconds()
            report["Total runtime"] = f"{overall_runtime / 60:.2f} minutes"

            # Initialize variables
            current_area = None
            area_start_time = None
            sub_entry_start_time = None
            sub_entry_message = None  # Track the message for sub-entries

            for i, log in enumerate(logs):
                message, area, log_time, level = log

                # Detect area change
                if area != current_area:
                    if current_area is not None:
                        # Calculate runtime for the previous area
                        area_end_time = log_time
                        area_runtime = (area_end_time - area_start_time).total_seconds()
                        if current_area not in report:
                            report[current_area] = {'Total_runtime': 0, 'Entries': {}}
                        report[current_area]['Total_runtime'] += area_runtime / 60

                        # Finalize the last sub-entry if any
                        if sub_entry_start_time is not None:
                            sub_entry_end_time = log_time
                            sub_entry_runtime = (sub_entry_end_time - sub_entry_start_time).total_seconds()
                            report[current_area]['Entries'][sub_entry_message] = f"{sub_entry_runtime / 60:.2f} minutes"

                    # Update for the new area
                    current_area = area
                    area_start_time = log_time
                    sub_entry_start_time = None  # Reset sub-entry start time for new area

                # Initialize the area in the report if not already done
                if current_area not in report:
                    report[current_area] = {'Total_runtime': 0, 'Entries': {}}

                # Handle status log entries
                if level == 'STATUS':
                    if sub_entry_start_time is not None:
                        # Finalize the current sub-entry
                        sub_entry_end_time = log_time
                        sub_entry_runtime = (sub_entry_end_time - sub_entry_start_time).total_seconds()
                        report[current_area]['Entries'][sub_entry_message] = f"{sub_entry_runtime / 60:.2f} minutes"
                    else:
                        # First STATUS call is the "Startup" phase
                        startup_runtime = (log_time - area_start_time).total_seconds()
                        report[current_area]['Entries']['Startup'] = f"{startup_runtime / 60:.2f} minutes"

                    # Start a new sub-entry
                    sub_entry_start_time = log_time
                    sub_entry_message = message  # Capture the message for the sub-entry

            # Finalize the last area
            if current_area is not None:
                area_end_time = logs[-1][2]
                area_runtime = (area_end_time - area_start_time).total_seconds()
                report[current_area]['Total_runtime'] += area_runtime / 60

                # Finalize the last sub-entry if any
                if sub_entry_start_time is not None:
                    sub_entry_end_time = logs[-1][2]
                    sub_entry_runtime = (sub_entry_end_time - sub_entry_start_time).total_seconds()
                    report[current_area]['Entries'][sub_entry_message] = f"{sub_entry_runtime / 60:.2f} minutes"

        # Convert runtime to string with 2 decimal places
        for area in report:
            if area != "Total runtime":
                report[area]['Total_runtime'] = f"{report[area]['Total_runtime']:.2f} minutes"

        # Modify the report format if there are no sub-entries
        for area in list(report.keys()):
            if area != "Total runtime" and not report[area]['Entries']:
                report[area] = f"{report[area]['Total_runtime']} minutes"
            elif area != "Total runtime":
                report[area]['Entries'] = {k: v for k, v in report[area]['Entries'].items()}

        with open('report_runtime.json', 'w') as f:
            json.dump(report, f, indent=4)

        return report

        
    def _generate_error_report(self):
        report = {}
        with self.engine.connect() as conn:
            cursor = conn.execute(text(f"SELECT DISTINCT area_name FROM logs WHERE instance_id = '{self.etl_id}'"))
            areas = [area[0] for area in cursor.fetchall()]

            for area in areas:
                levels_list = ', '.join([f"'{level}'" for level in self.reported_levels])
                query = f"SELECT level FROM logs WHERE area_name = '{area}' AND level IN ({levels_list}) AND instance_id = '{self.etl_id}'"
                
                cursor = conn.execute(text(query))
                area_logs = cursor.fetchall()

                error_counts = {level: 0 for level in self.reported_levels}
                for log in area_logs:
                    error_counts[log[0]] += 1
                
                filtered_errors = {level: {'Count': count} for level, count in error_counts.items() if count > 0}
                
                if not filtered_errors:
                    report[area] = "No Errors"
                else:
                    report[area] = filtered_errors


        with open('report_errors.json', 'w') as f:
            json.dump(report, f, indent=4)
        
        return report

### Example usage
if __name__ == '__main__':

    import sqlite3

    logger = get_logger(__name__)
    logger.info("This is an info message")

    time.sleep(2)

    logger = get_logger("Candidate_scraper")
    logger.info("This is an info message")
    logger.error("This is an error message")
    logger.warning("This is a warning message")

    time.sleep(2)

    logger = get_logger("Job_scraper")
    logger.info("This is an info message")
    logger.error("This is an error message")
    time.sleep(2)

    logger.status("Starting important work")
    time.sleep(3)
    logger.warning("This is a warning message")
    time.sleep(7)

    time.sleep(2)

    logger = get_logger("Good_scraper")
    logger.info("This is an info message")
    time.sleep(2)

    logger.info("This is also an info message")
