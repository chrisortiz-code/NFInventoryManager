import json
import os
import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import psycopg2
import pyautogui
import time
from datetime import datetime, timedelta, date
import matplotlib
from matplotlib import pyplot as plt

matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import matplotlib.dates as mdates
from tkinter import ttk  # For the Progressbar


class FiltererApp:
    def __init__(self, root):
        self.root = root
        self.root.title("0 Filterer Server Edition")

        # ------------------------ Class Variables ------------------------
        self.df_inventory = pd.DataFrame()  # Will hold your full Excel in-memory
        self.filtered_zeros = set()  # Articles with inventory <= 0 (not in DNO)
        self.filtered_lows = set()  # Articles with 0 < inventory <= low threshold
        self.zero_article_count = 0
        self.new_found_dnos = 0

        self.zero_button = None
        self.low_button = None
        self.inputted = False  # for logging (if you use logs)

        # NEW: Track whether we've already sent inventory to postgres
        self.sent_to_postgres = False

        # Low threshold hyperparameter
        self.LOW_THRESHOLD = 2

        # Departments dictionary -> for the "lights" in UI
        self.departments = {
            "Grocery": ["Grocery"],
            "Meat": ["Meat", "Deli"],
            "Bakery": ["Bakery Commercial", "Bakery Instore"],
            "Dairy/Frozen": ["Bulk"],
            "Seafood": ["Seafood"],
            "HMR": ["HMR"],
            "Produce": ["Produce"],
            "Home": ["Home", "Entertainment"]
        }

        # Banned categories (not to be reported)
        self.BANNED_CATS = [
            # Produce
            "Nuts/ Dried Fruit", "Fresh-", "Field Veg", "Root Veg", "Salad Veg",
            "Cooking Veg", "Peppers", "Tomatoes",
            # Meat
            "Lamb", "Sausage", "Hams",
            # Entertainment
            "Books-", "Magazines", "Newspapers"
        ]

        self.lights_bool = {dep: False for dep in self.departments.keys()}

        self.db_config = json.load(open('config.json'))


        self.conn = None

        # ------------------------ UI SETUP ------------------------
        #
        # 1) DEPARTMENT LIGHTS FRAME (top)
        #
        self.dept_frame = tk.Frame(root)
        self.dept_frame.grid_columnconfigure((0, 2), weight=1)
        self.dept_frame.pack(pady=20, padx=20)

        self.buttons = {}
        self.lights = {}
        for idx, department_name in enumerate(self.departments):
            lbl = tk.Label(self.dept_frame, text=department_name)
            lbl.grid(row=idx // 2, column=2 * (idx % 2), padx=10, pady=5)
            self.buttons[department_name] = lbl

            light = tk.Canvas(self.dept_frame, width=20, height=20)
            light.create_oval(2, 2, 18, 18, fill="red", tags="light")
            light.grid(row=idx // 2, column=2 * (idx % 2) + 1)
            self.lights[department_name] = light

        #
        # 2) MAIN CONTROL FRAME (split into two segments side by side)
        #
        self.control_frame = tk.Frame(root)
        self.control_frame.pack(pady=20, padx=20)

        # Left side: DNO Management
        self.dno_frame = tk.LabelFrame(self.control_frame, text="DNO Management", padx=10, pady=10)
        self.dno_frame.grid(row=0, column=0, sticky="n")

        tk.Label(self.dno_frame, text="Article #:").grid(row=0, column=0, padx=5, pady=(0, 5))
        self.entry = tk.Entry(self.dno_frame)
        self.entry.grid(row=1, column=0, padx=5, pady=(0, 10))

        self.add_ONE_btn = tk.Button(self.dno_frame, text="Add to DNO", command=self.add_new_DNO)
        self.add_ONE_btn.grid(row=2, column=0, padx=5, pady=3, sticky="ew")

        self.remove_ONE_btn = tk.Button(self.dno_frame, text="Remove from DNO", command=self.remove_from_DNO)
        self.remove_ONE_btn.grid(row=3, column=0, padx=5, pady=3, sticky="ew")

        # Right side: Inventory & Filters
        self.inv_frame = tk.LabelFrame(self.control_frame, text="Inventory & Filters", padx=10, pady=10)
        self.inv_frame.grid(row=0, column=1, sticky="n", padx=(20, 0))

        self.upload_button = tk.Button(self.inv_frame, text="Upload Excel", command=self.upload_excel)
        self.upload_button.grid(row=0, column=0, padx=5, pady=5, sticky="ew")

        self.find_zeros_btn = tk.Button(self.inv_frame, text="Find Zeros", command=self.find_zeros)
        self.find_zeros_btn.grid(row=1, column=0, padx=5, pady=5, sticky="ew")

        self.find_lows_btn = tk.Button(self.inv_frame, text="Find Lows", command=self.find_lows)
        self.find_lows_btn.grid(row=2, column=0, padx=5, pady=5, sticky="ew")

        self.send_to_server_btn = tk.Button(
            self.inv_frame,
            text="Send Inventory to Server",
            command=self.open_send_inventory_window,
            state=tk.NORMAL if not self.sent_to_postgres else tk.DISABLED
        )
        self.send_to_server_btn.grid(row=5, column=0, padx=5, pady=5, sticky="ew")

        self.graph_button = tk.Button(
            self.inv_frame,
            text="View Product History",
            command=self.open_time_series_window
        )
        self.graph_button.grid(row=6, column=0, padx=5, pady=5, sticky="ew")

        # Final window close protocol
        self.root.protocol("WM_DELETE_WINDOW", self.close_app)


    # ----------------- Conn Helpers -----------------

    def get_cursor(self):
        """
        Returns a (cursor, conn).
        If no existing connection, create one from self.db_config.
        """
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(
                    host=self.db_config['host'],
                    dbname=self.db_config['dbname'],
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    port=self.db_config['port'],
                )
            except psycopg2.Error as e:
                self.show_alert(f"Error connecting to Server")
                return None, None
        cur = self.conn.cursor()
        return cur, self.conn

    def close_conn(self, cur):
        """
        Closes the given cursor, then closes self.conn if open,
        finally sets self.conn = None.
        """
        if cur:
            cur.close()
        if self.conn:
            self.conn.close()
            self.conn = None



    # ----------------- DNO: Add & Remove -----------------
    def add_new_DNO(self):
        newdno = self.entry.get().strip()
        if not newdno:
            self.show_alert("Article number cannot be empty.", "Input Error")
            return

        confirm = messagebox.askokcancel(
            "Confirm Action",
            f"Are you sure you want to insert {newdno} as a DNO?\n\nPlease verify the article number carefully."
        )
        if not confirm:
            return

        cur, conn = self.get_cursor()
        if cur is None:
            self.show_alert("Failed to connect to the database.", "Connection Error")
            return

        try:
            # PostgreSQL upsert: Insert new row or update 'active' to TRUE if it exists
            upsert_query = """
                INSERT INTO dno (article, active)
                VALUES (%s, TRUE)
                ON CONFLICT (article)
                DO UPDATE SET active = TRUE
            """
            cur.execute(upsert_query, (newdno,))
            conn.commit()

            # Check if a new row was inserted or an existing row was updated
            if cur.rowcount == 1:
                self.show_alert(f"Article {newdno} has been added to DNO.", "Inserted")
            elif cur.rowcount == 0:
                self.show_alert(f"Article {newdno} was already active in DNO.", "Already Active")

            self.new_found_dnos += 1

        except psycopg2.Error as e:
            self.show_alert(f"Database error: {e}", "Error")
        finally:
            self.close_conn(cur)

    def fetch_dno_articles(self):
        """
        Returns a list of articles stored in the remote DNO table.
        """
        cur, _ = self.get_cursor()
        try:
            cur.execute("SELECT article FROM dno")
            rows = cur.fetchall()
            return [int(r[0]) for r in rows]
        finally:
            self.close_conn(cur)

    def remove_from_DNO(self):
        bad_dno = self.entry.get().strip()
        if not bad_dno:
            return
        confirm = messagebox.askokcancel(
            "Confirm Action",
            f"Set active to FALSE for Article {bad_dno}? Read the article number carefully."
        )
        if confirm:
            cur, conn = self.get_cursor()
            if cur is None:
                self.show_alert("Failed to connect to the database.", "Connection Error")
                return
            try:
                # Update the 'active' column to FALSE for the specified article
                cur.execute("UPDATE dno SET active = FALSE WHERE article = %s", (bad_dno,))
                conn.commit()

                if cur.rowcount > 0:
                    self.show_alert(f"Article {bad_dno} has been deactivated.", "Article Deactivated")
                    self.new_found_dnos += 1
                else:
                    self.show_alert(f"Article {bad_dno} was not found or is already inactive.", "Article Not Found")
            except psycopg2.Error as e:
                self.show_alert(f"Database error: {e}", "Error")
            finally:
                self.close_conn(cur)

    # ----------------- Excel Upload & Department Lights -----------------
    def upload_excel(self):
        file_path = filedialog.askopenfilename(filetypes=[("Excel Workbooks", "*.xlsx")])
        if not file_path:
            return

        new_df = pd.read_excel(file_path, engine='openpyxl')

        # Department-lights logic (reading from "Department"?)
        if "Department" in new_df.columns:
            departments_in_file = new_df["Department"]
            for dep in departments_in_file:
                for outer_key, inner_values in self.departments.items():
                    if dep in inner_values:
                        self.lights_bool[outer_key] = True
                        self.lights[outer_key].itemconfig("light", fill="green")
                        break

        # Filter out banned categories & store final result in memory
        columns_needed = ['Department', 'Merchandise Category', 'Article Description', 'Article', 'Inventory']

        new_df = new_df[columns_needed].copy()

        # Function to check if a category is banned
        def is_banned(cat):
            return any(cat.startswith(bad) for bad in self.BANNED_CATS if isinstance(cat, str))

        new_df = new_df[~new_df["Merchandise Category"].apply(is_banned)].reset_index(drop=True)

        # Append new data to existing data
        if self.df_inventory.empty:
            self.df_inventory = new_df
        else:
            self.df_inventory = pd.concat([self.df_inventory, new_df], ignore_index=True)

        # Drop duplicates by Article, keep last
        if "Article" in self.df_inventory.columns:
            self.df_inventory.drop_duplicates(subset=["Article"], keep="last", inplace=True, ignore_index=True)


    # ----------------- Find Zeros & Lows -----------------
    def find_zeros(self):
        if self.df_inventory.empty:
            self.show_alert("No inventory loaded. Please upload Excel first.", "Error")
            return

        zero_inventory_df = self.df_inventory[self.df_inventory["Inventory"] <= 0].copy()
        dno_articles = self.fetch_dno_articles()
        zero_inventory_df = zero_inventory_df[~zero_inventory_df["Article"].isin(dno_articles)]

        unique_zero_articles = zero_inventory_df["Article"].dropna().unique()
        self.filtered_zeros.update(int(article) for article in unique_zero_articles)

        zero_count = len(self.filtered_zeros)
        self.update_zero_text(zero_count)

        self.show_alert("Zero-inventory articles processed.\nReady to send to SAP.", "Success")

    def find_lows(self):
        if self.df_inventory.empty:
            self.show_alert("No inventory loaded. Please upload Excel first.", "Error")
            return

        low_inventory_df = self.df_inventory[
            (self.df_inventory["Inventory"] > 0) &
            (self.df_inventory["Inventory"] <= self.LOW_THRESHOLD)
            ].copy()

        unique_low_articles = low_inventory_df["Article"].dropna().unique()
        self.filtered_lows.update(int(article) for article in unique_low_articles)

        low_count = len(self.filtered_lows)
        self.update_low_text(low_count)

        self.show_alert("Low-inventory articles found.\nReady to send to SAP.", "Success")

    # ----------------- Send to SAP -----------------
    def send_to_SAP(self, mode=0):
        entryx = 222
        entryy = 330

        def process_lines(data_list):
            if not data_list:
                return
            line = str(data_list.pop(0)).strip()
            if line:
                pyautogui.click(entryx, entryy)
                pyautogui.click(entryx, entryy)
                time.sleep(1.02)
                pyautogui.write(line)
                pyautogui.press('enter')
                time.sleep(0.5)
                pyautogui.press('enter')
                time.sleep(0.5)
            process_lines(data_list)

        if mode == 1:
            file_length = len(self.filtered_lows)
            confirm = messagebox.askokcancel(
                "Confirm Action",
                f"Make sure the SAP window is in the far-left position.\nETA: {file_length * 2 // 60} mins.",
                parent=self.root
            )
            if confirm:
                time.sleep(2)
                data_to_process = list(self.filtered_lows)
                process_lines(data_to_process)
                self.show_alert("Low-inventory articles sent to SAP.", "Done")
        else:
            file_length = len(self.filtered_zeros)
            confirm = messagebox.askokcancel(
                "Confirm Action",
                f"Make sure the SAP window is in the far-left position.\nETA: {file_length * 2 // 60} mins.",
                parent=self.root
            )
            if confirm:
                time.sleep(3)
                data_to_process = list(self.filtered_zeros)
                process_lines(data_to_process)
                self.show_alert("Zero-inventory articles sent to SAP.", "Done")

    # ----------------- Button Label Updates -----------------
    def update_low_text(self, article_count: int):
        if self.low_button:
            self.low_button.destroy()
        self.low_button = tk.Button(
            self.inv_frame,
            text=f"Send {article_count} Lows to SAP",
            command=lambda: self.send_to_SAP(1)
        )
        self.low_button.grid(row=3, column=0, padx=5, pady=5, sticky="ew")

    def update_zero_text(self, article_count: int):
        if self.zero_button:
            self.zero_button.destroy()
        self.zero_button = tk.Button(
            self.inv_frame,
            text=f"Send {article_count} 0's to SAP",
            command=self.send_to_SAP
        )
        self.zero_button.grid(row=4, column=0, padx=5, pady=5, sticky="ew")

    def open_send_inventory_window(self):
        # If inventory is empty or if we've already sent, just bail
        if self.df_inventory.empty:
            self.show_alert("No inventory to send. Upload an Excel first.", "Error")
            return
        if self.sent_to_postgres:
            return

        # Create a new Toplevel window for the pipeline
        InventoryPipeline(self.root, self.df_inventory, self.db_config, parent_app=self)
        self.sent_to_postgres = True

    def open_time_series_window(self):
        """
        Opens a new Toplevel window that lets the user input:
          - Article ID
          - Start Week
          - End Week
        Then queries postgres to fetch the time series from DailyCheckIn + Products
        and plots the results in a Matplotlib figure.
        """
        # Create the Toplevel
        self.top_ts = tk.Toplevel(self.root)
        self.top_ts.title("Time Series Options")

        # For convenience, store some default values
        current_week = datetime.now().isocalendar()[1]

        # Labels and Entries
        tk.Label(self.top_ts, text="Article ID:").grid(row=0, column=0, padx=5, pady=5, sticky="e")
        article_entry = tk.Entry(self.top_ts)
        article_entry.grid(row=0, column=1, padx=5, pady=5, sticky="w")

        tk.Label(self.top_ts, text="Start Week:").grid(row=1, column=0, padx=5, pady=5, sticky="e")
        start_entry = tk.Entry(self.top_ts)
        start_entry.insert(0, "0")  # Default to week 0
        start_entry.grid(row=1, column=1, padx=5, pady=5, sticky="w")

        tk.Label(self.top_ts, text="End Week:").grid(row=2, column=0, padx=5, pady=5, sticky="e")
        end_entry = tk.Entry(self.top_ts)
        end_entry.insert(0, str(current_week))  # Default to current ISO week
        end_entry.grid(row=2, column=1, padx=5, pady=5, sticky="w")

        # Button to execute the query and plot
        plot_button = tk.Button(
            self.top_ts,
            text="Plot Time Series",
            command=lambda: self.plot_time_series(
                article_entry.get(),
                start_entry.get(),
                end_entry.get()
            )
        )
        plot_button.grid(row=3, column=0, columnspan=2, padx=10, pady=10, sticky="ew")

    def iso_to_date(self, iso_year: int, iso_week: int, iso_day: int):
        """
        Convert an ISO year-week-day combination (e.g. 2025, 2, 2)
        into a Python date object (2025-01-07).

          - iso_year: The ISO-8601 year (which can differ from the calendar year near boundaries)
          - iso_week: The ISO week number (1..53)
          - iso_day:  The ISO weekday (1=Monday .. 7=Sunday)

        Returns:
            A datetime.date object representing the actual (Gregorian) date.
        """
        # Step 1: January 4 is always in ISO week 1.
        january_4 = date(iso_year, 1, 4)

        # Step 2: Find Monday of ISO week 1
        # isoweekday() => Monday=1, Tuesday=2, ... Sunday=7
        first_monday = january_4 - timedelta(days=january_4.isoweekday() - 1)

        # Step 3: From that Monday, move forward (iso_week - 1) weeks and (iso_day - 1) days
        return first_monday + timedelta(weeks=(iso_week - 1), days=(iso_day - 1))

    def fetch_time_series(self, article_id, start_week, end_week):
        """
        Fetches inventory data from the database for the specified article
        and week range. Returns (rows, product_description, article_id).

        Each row in 'rows' will look like:
          (year, week, D0_inventory, D1_inventory, D2_inventory, D3_inventory, D4_inventory, D5_inventory, D6_inventory)
        """
        conn = None
        try:
            cur, conn = self.get_cursor()
            if cur is None:
                return None, None, None

            # Fetch product description
            description_query = "SELECT description FROM Products WHERE article_number = %s"
            cur.execute(description_query, (article_id,))
            description_result = cur.fetchone()
            if not description_result:
                self.show_alert(f"No product found for Article ID: {article_id}", "Error")
                return None, None, None
            product_description = description_result[0]

            # Fetch time-series data
            sql_query = """
            SELECT DC.year, DC.week, DC.D0_inventory, DC.D1_inventory, DC.D2_inventory,
                   DC.D3_inventory, DC.D4_inventory, DC.D5_inventory, DC.D6_inventory
            FROM DailyCheckIn AS DC
            JOIN Products AS P ON DC.product_id = P.id
            WHERE P.article_number = %s
              AND DC.week >= %s
              AND DC.week <= %s
            ORDER BY DC.year, DC.week
            """
            cur.execute(sql_query, (article_id, start_week, end_week))
            rows = cur.fetchall()

            return rows, product_description, article_id

        except psycopg2.Error as e:
            self.show_alert(str(e), "PostgreSQL Error")
            return None, None, None
        finally:
            self.close_conn(cur)


    def plot_time_series(self, article_str, start_week_str, end_week_str):
        """
        Plots the inventory time series for the given article and week range.

        Args:
            article_str (str): The article number.
            start_week_str (str): The starting week number as a string.
            end_week_str (str): The ending week number as a string.
        """
        # Convert week strings to integers, handle invalid inputs
        try:
            start_week = int(start_week_str)
        except ValueError:
            start_week = 0

        try:
            end_week = int(end_week_str)
        except ValueError:
            end_week = datetime.now().isocalendar()[1]

        # Fetch data
        rows, description, article_id = self.fetch_time_series(article_str, start_week, end_week)
        if not rows:
            return

        # Prepare data for plotting
        plot_dates = []
        plot_inventories = []

        for (yr, wk, D0, D1, D2, D3, D4, D5, D6) in rows:
            daily_invs = [D0, D1, D2, D3, D4, D5, D6]
            for day_num, inventory in enumerate(daily_invs):
                if inventory is None:
                    continue  # Skip None inventories to ensure last point is valid
                iso_day = day_num + 1  # D0=1 (Monday), D1=2 (Tuesday), ..., D6=7 (Sunday)

                dt = self.iso_to_date(yr, wk, iso_day)

                plot_dates.append(dt)
                plot_inventories.append(inventory)


        # Sort the data by date
        paired = sorted(zip(plot_dates, plot_inventories), key=lambda x: x[0])
        sorted_dates, sorted_inventories = zip(*paired)

        # Create a chart window
        chart_window = tk.Toplevel(self.root)
        chart_window.title(f"Time Series for Article {article_str}")

        # Create a Matplotlib figure with dark background
        fig = Figure(figsize=(10, 6), dpi=100, facecolor='black')
        ax = fig.add_subplot(111)

        # Plot the data
        ax.plot(
            sorted_dates, sorted_inventories,
            marker='o', linestyle='-', color='red', label="Inventory"
        )

        # Set facecolor to match the figure
        ax.set_facecolor('black')

        # Set title and labels with white color for visibility on dark background
        ax.set_title(
            f"{description} (Article {article_str}) Inventory Over Time",
            fontsize=16, color='white'
        )
        ax.set_xlabel("Date", fontsize=12, color='white')
        ax.set_ylabel("Inventory", fontsize=12, color='white')

        # Configure x-axis with dynamic date labels
        day_span = (sorted_dates[-1] - sorted_dates[0]).days
        if day_span <= 14:
            # Label every day
            locator = mdates.DayLocator(interval=1)
            formatter = mdates.DateFormatter('%Y-%m-%d')
        else:
            # Label weekly on Mondays
            locator = mdates.WeekdayLocator(byweekday=mdates.MO)
            formatter = mdates.DateFormatter('%Y-%m-%d')

        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(formatter)

        # Rotate x-tick labels for better readability
        plt.setp(ax.get_xticklabels(), rotation=45, ha='right', color='white')

        # Set y-tick labels color
        ax.tick_params(axis='y', colors='white', labelsize=10)

        # Enable grid with lighter color
        ax.grid(True, which='major', linestyle='--', linewidth=0.5, color='gray')

        # Add legend with white text
        legend = ax.legend(fontsize=12, loc="upper right")
        for text in legend.get_texts():
            text.set_color("white")

        # Adjust layout to prevent clipping of tick-labels
        fig.tight_layout()

        # Embed the plot in tkinter
        canvas = FigureCanvasTkAgg(fig, master=chart_window)
        canvas.draw()
        canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # Optionally, you can add a toolbar
        # toolbar = NavigationToolbar2Tk(canvas, chart_window)
        # toolbar.update()
        # canvas.get_tk_widget().pack()
    # ----------------- Closing & Logs -----------------

    def show_alert(self, message, title="Information"):
        messagebox.showinfo(title, message)
    def close_app(self):
        """
        1) If self.sent_to_postgres is still False, we run the pipeline (so data isn't lost).
        2) Then handle logging, etc.
        3) Finally, destroy the root window.
        """
        if not self.sent_to_postgres and not self.df_inventory.empty:
            # Attempt to send data automatically
            # We'll do it *without* the Toplevel UI in this forced scenario,
            # but you can also do it with Toplevel if you want the user to see the progress.
            InventoryPipeline(self.root, self.df_inventory, self.db_config, parent_app=self, auto_mode=True)

        # 2) Optional logging if self.inputted is used:
        if self.inputted:
            log_message = (
                f"Session Ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Zeros sent: {len(self.filtered_zeros)}\n"
                f"Lows sent: {len(self.filtered_lows)}\n"
                f"{self.new_found_dnos} new DNO articles were tracked.\n"
                "--------------------------------------------\n"
            )
            with open("log.txt", "a") as f:
                f.write(log_message)

        # 3) Destroy the app
        self.root.destroy()

import threading
class InventoryPipeline(tk.Toplevel):
    """
    Toplevel window to send the DataFrame's inventory to postgres row-by-row,
    displaying a progress bar and logging newly discovered products
    in a text box.

    If auto_mode=True, we won't actually show this window. Instead,
    we do the insertion quietly (used on close_app).
    """

    def __init__(self, master, df_inventory, db_config, parent_app, auto_mode=False, *args, **kwargs):
        super().__init__(master, *args, **kwargs)
        self.master = master
        self.parent_app = parent_app
        self.df_inventory = df_inventory
        self.db_config = db_config
        self.auto_mode = auto_mode

        # If auto_mode is False, we build the GUI
        if not self.auto_mode:
            self.title("Sending Inventory to Server")
            self.geometry("500x400")

            self.progress_label = ttk.Label(self, text="Progress:")
            self.progress_label.pack(pady=(15, 0))

            self.progress_bar = ttk.Progressbar(self, orient="horizontal", length=400, mode="determinate")
            self.progress_bar.pack(pady=5)

            # A text box to log newly discovered products
            self.log_text = tk.Text(self, width=60, height=12)
            self.log_text.pack(pady=10)

        # Start the pipeline
        threading.Thread(target=self.send_data_to_postgres).start()

    def send_data_to_postgres(self):
        total_rows = len(self.df_inventory)
        progress_val = 0

        try:
            conn = psycopg2.connect(
                host=self.db_config['host'],
                dbname=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                port=self.db_config['port'],
            )
            cur = conn.cursor()
        except psycopg2.Error as e:
            messagebox.showerror("PostgreSQL Error", str(e), parent=self.master)
            self.destroy()
            return

        try:
            # Get current date details
            today = datetime.now()
            current_year = today.year
            current_week = today.isocalendar()[1]
            current_weekday = today.weekday()  # 0=Monday, 6=Sunday

            # Map current_weekday to D0 to D6
            day_column = f"D{current_weekday}_inventory"

            for i, row in self.df_inventory.iterrows():
                department = row.get("Department", "")
                category = row.get("Merchandise Category", "")
                description = row.get("Article Description", "")
                article = str(row.get("Article", None))
                inventory = row.get("Inventory", None)

                if pd.isna(article):
                    continue

                # Check if product exists
                cur.execute("SELECT id FROM Products WHERE article_number = %s", (article,))
                product = cur.fetchone()

                if not product:
                    # Insert as new product
                    cur.execute("""
                        INSERT INTO Products (article_number, description, department, category)
                        VALUES (%s, %s, %s, %s)
                        RETURNING id
                    """, (article, description, department, category))
                    product_id = cur.fetchone()[0]
                    conn.commit()

                    if not self.auto_mode:
                        self.log_text.insert(tk.END, f"New product discovered: {description}\n")
                        self.log_text.see(tk.END)
                else:
                    product_id = product[0]

                # Upsert into DailyCheckIn
                cur.execute("""
                    INSERT INTO DailyCheckIn (product_id, year, week, {day_col})
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (product_id, year, week)
                    DO UPDATE SET {day_col} = EXCLUDED.{day_col}
                """.format(day_col=day_column),
                            (product_id, current_year, current_week, inventory))
                conn.commit()

                # Update progress
                progress_val += 1
                if not self.auto_mode:
                    self.progress_bar['value'] = progress_val
                    self.progress_bar['maximum'] = total_rows
                    if total_rows > 0:
                        pct = int((progress_val / total_rows) * 100)
                        self.progress_label.config(text=f"Progress: {pct}%")
                    self.update_idletasks()

        except psycopg2.Error as e:
            messagebox.showerror("PostgreSQL Error", str(e), parent=self.master)
        finally:
            cur.close()
            conn.close()

        self.parent_app.sent_to_postgres = True
        self.parent_app.send_to_server_btn.config(state=tk.DISABLED)
        self.destroy()


# ----------------- MAIN -----------------
if __name__ == "__main__":
    root = tk.Tk()
    root.geometry("600x500+800+400")
    app = FiltererApp(root)
    root.mainloop()
