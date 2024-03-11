import sqlite3
import json
import os
import time
from datetime import datetime
import threading

class BaseLogger:
    """
    This is a class for handling the logging process of a larger application.
    
    It is built around using a SQLite database to store the logs concurrently, allowing for more detailed and complex logging than what is easily done with
    the built-in Python logging module.

    Notably, it includes an automatic runtime and error report feature.
    See the start() method for configuration options.

    Args:
        area (str): The area of the application that the logger is being used for. Defaults to "overall" for initial setup.
    """

    def __init__(self, area="overall"):

        self.area = area

        self.db_name = "logs"
        self.db_path = f'{self.db_name}.db'
        self._set_level("info")
        self.report_as_called = True

        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

        self._create_table()

        self._log("timestamp", f"Logger started for {area}")

    def __getattr__(self, level):
        """
        This magic method is called when an attribute that doesn't exist is accessed.
        We use it to handle logging levels dynamically. e.g., if CLogger.info is called, level = "info".
        """
        def method(message):
            self._log(level, message)
        return method

    def start(self, db_name="logs", level_to_report = "warning", report_as_called=False):
        """This method configures the logger and ensures the db/table exists.
        
        Args:
            db_name (str): The name of the database to use.
            level_to_report (str): The level to report.
            report_as_called (bool): Whether to print the logs as they are called.
        """

        settings = {}
        settings["db_name"] = db_name
        settings["level_to_report"] = level_to_report
        settings["report_as_called"] = report_as_called

        print(f"Starting logger with settings: {self.settings}")

        try:
            with open('.cache/settings.json', 'w') as file:
                json.dump(settings, file, indent=4)
        except FileNotFoundError:
            os.makedirs('.cache', exist_ok=True)
            with open('.cache/settings.json', 'w') as file:
                json.dump(settings, file, indent=4)

        self.cursor.execute("DELETE FROM logs")
        self.conn.commit()

    def __del__(self):
        """
        This magic method is called when the object is deleted.
        We use it to close the database connection.
        """

        self._log("timestamp", f"Logger ended for {self.area}")
        self.conn.close()

    def _delete_table(self):
        """
        This method deletes the table in the database.
        """
        self.cursor.execute("DROP TABLE logs")
        self.conn.commit()
        print("Table deleted")

    def _create_table(self):
        """
        This method creates the table in the database for storing the logs.
        """
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                area TEXT, 
                level TEXT, 
                message TEXT, 
                inserted_at TIMESTAMP 
            )
        """) # DEFAULT CURRENT_TIMESTAMP
        self.conn.commit()
        

    def _set_level(self, level_to_report):
        """
        This method sets the level of the logger.

        Args:
            level (str): The level to set the logger to.
        """
        usable_levels = ["critical", "error", "warning", "info", "debug", "timestamp"]
        if level_to_report not in usable_levels:
            raise ValueError(f"Level must be one of {usable_levels}")
        
        level_index = usable_levels.index(level_to_report)
        self.reported_levels = usable_levels[:level_index + 1]

    def _log(self, level, message):
        """
        This method logs a message to the database with the specified level.

        Args:
            level (str): The level of the log.
            message (str): The message to be logged.
        """

        if self.report_as_called == True & (level in self.reported_levels):
                print(f"{level.upper()}: {message}")

        self.cursor.execute(
            "INSERT INTO logs (area, level, message, inserted_at) VALUES (?, ?, ?, ?)", 
            (self.area, level, message, str(datetime.now())))
        self.conn.commit()

    def _generate_runtime_report(self):
        """This method generates a report of the runtime of each area in the database."""

        report = {}

        # Getting overall runtime
        self.cursor.execute("SELECT message, inserted_at FROM logs ORDER BY inserted_at")
        timestamps_entries = self.cursor.fetchall()
        if len(timestamps_entries) < 2:
            raise ValueError("There must be at least two timestamp entries to generate a runtime report")
        start = datetime.fromisoformat(timestamps_entries[0][1])
        end = datetime.fromisoformat(timestamps_entries[-1][1])
        overall_runtime = (end - start).total_seconds()
        report["Total runtime"] = overall_runtime

        # Selecting unique areas from the logs table
        self.cursor.execute("SELECT DISTINCT area FROM logs ORDER BY inserted_at")
        areas = self.cursor.fetchall()

        for area_tuple in areas:
            area = area_tuple[0]
            report[area] = {'total_runtime': 0, 'entries': []}

            self.cursor.execute("SELECT message, inserted_at FROM logs WHERE area = ? ORDER BY inserted_at", (area,))
            timestamps_entries = self.cursor.fetchall()

            # Assuming the first and last entries indicate the total runtime for the area
            area_start = datetime.fromisoformat(timestamps_entries[0][1])
            area_end = datetime.fromisoformat(timestamps_entries[-1][1])
            area_runtime = (area_end - area_start).total_seconds()
            report[area]['total_runtime'] = area_runtime

            self.cursor.execute("SELECT message, inserted_at FROM logs WHERE area = ? AND level = 'timestamp' ORDER BY inserted_at", (area,))
            timestamps_entries = self.cursor.fetchall()

            # Calculating runtimes for each entry within an area
            for i in range(len(timestamps_entries) - 1):
                entry_start = datetime.fromisoformat(timestamps_entries[i][1])
                entry_end = datetime.fromisoformat(timestamps_entries[i + 1][1])
                entry_runtime = (entry_end - entry_start).total_seconds()
                report[area]['entries'].append({'message': timestamps_entries[i][0], 'runtime': entry_runtime})

        # Outputting to a JSON file
        with open('runtime_report.json', 'w') as f:
            json.dump(report, f, indent=4)
        
    def _generate_error_report(self):
        """
        This method generates a report of the logs in the database.
        """
        
        # Deleting the previous report
        try:
            os.remove('report.json')
        except FileNotFoundError:
            pass

        report = {}

        # Fetching all unique areas
        self.cursor.execute("SELECT DISTINCT area FROM logs ORDER BY inserted_at desc")
        areas = [area[0] for area in self.cursor.fetchall()]

        for area in areas:
            # Fetching logs for the area that match the specified levels
            placeholders = ', '.join(['?'] * len(self.reported_levels))
            query = f"SELECT level, message FROM logs WHERE area = ? AND level IN ({placeholders})"
            self.cursor.execute(query, (area, *self.reported_levels))
            area_logs = self.cursor.fetchall()

            # If no logs match the specified levels, indicate the area was completed successfully
            if area_logs:
                report[area] = [{'level': log[0], 'message': log[1]} for log in area_logs]
            else:
                report[area] = "Completed successfully"
            
        # Writing the report to a JSON file
        with open('report.json', 'w') as f:
            json.dump(report, f, indent=4)
        return report

    def generate_reports(self):

        """This method generates the runtime and error reports and then deletes the logs from the database to reset the logger."""
        self._log("timestamp", f"Logger ended")
        try:
            self._generate_runtime_report()
            self._generate_error_report()
        except ValueError as e:
            print(f"Error generating reports: {e}")
            pass
        try:
            self.cursor.execute("DELETE FROM logs")
            self.conn.commit()
        except Exception as e:
            print(f"Error resetting logs: {e}")
            pass

class Logger(BaseLogger):
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, area="overall"):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(Logger, cls).__new__(cls)
                    # Initialize BaseLogger directly here to ensure it's done once.
                    BaseLogger.__init__(cls._instance, area)
        return cls._instance


### Example usage
if __name__ == '__main__':

    logging = Logger()
    logging.start(db_name = "logs", level_to_report = "info", report_as_called = True)

    time.sleep(2)

    logging = Logger("Candidate_scraper")
    logging.info("This is an info message")
    logging.error("This is an error message")
    logging.warning("This is a warning message")

    time.sleep(2)

    logging = Logger("Job_scraper")
    logging.info("This is an info message")
    logging.error("This is an error message")
    time.sleep(2)
    logging.timestamp("Did the first two")
    logging.warning("This is a warning message")
    time.sleep(2)
    logging.timestamp("Did the last two")

    time.sleep(2)

    logging = Logger("Good_scraper")
    logging.info("This is an info message")
    time.sleep(2)

    logging.info("This is also an info message")

    logging.generate_reports()

