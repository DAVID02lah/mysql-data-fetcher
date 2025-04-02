import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import pyodbc
import pandas as pd
from tkinter import simpledialog
import importlib.util
import sys
import csv
import datetime

# Better MySQL module handling
mysql_connector = None
mysql_available = False

# Try to import MySQL connector safely
try:
    import mysql.connector
    mysql_connector = mysql.connector
    mysql_available = True
except ImportError:
    # Module is not available, but we'll handle this gracefully
    pass

# Define SQL operation constants
JOIN_TYPES = [
    {"label": "INNER JOIN", "value": "INNER JOIN"},
    {"label": "LEFT JOIN", "value": "LEFT JOIN"},
    {"label": "RIGHT JOIN", "value": "RIGHT JOIN"},
    {"label": "FULL OUTER JOIN", "value": "FULL OUTER JOIN"}
]

# Define aggregate functions
AGGREGATE_FUNCTIONS = [
    "COUNT", "SUM", "AVG", "MIN", "MAX"
]

# Define sorting orders
SORT_ORDERS = [
    "ASC", "DESC"
]


class SQLDataFetcher:
    def __init__(self, root):
        self.root = root
        self.root.title("MySQL Data Fetcher")
        self.root.geometry("1100x750")
        
        # Add these near the start of __init__ (e.g., just after self.root = root) to define styles & dark mode:
        style = ttk.Style()
        style.theme_use("default")

        style.configure(
            "Light.TFrame",
            background="#f0f0f0"
        )
        style.configure(
            "Light.TLabel",
            background="#f0f0f0",
            foreground="#333333"
        )
        style.configure(
            "Light.TButton",
            background="#e0e0e0",
            foreground="#000000"
        )

        style.configure(
            "Dark.TFrame",
            background="#2c2c2c"
        )
        style.configure(
            "Dark.TLabel",
            background="#2c2c2c",
            foreground="#ffffff"
        )
        style.configure(
            "Dark.TButton",
            background="#555555",
            foreground="#ffffff"
        )

        # Database connection variables
        self.server = tk.StringVar()
        self.database = tk.StringVar()
        self.username = tk.StringVar()
        self.password = tk.StringVar()
        self.auth_type = tk.StringVar(value="Windows Authentication")
        self.db_type = tk.StringVar(value="SQL Server")
        self.port = tk.StringVar(value="3306")  # Default MySQL port
        
        # Tables and columns
        self.tables = {}
        self.selected_tables = []
        self.selected_columns = {}
        
        # Active connection objects
        self.active_conn = None
        self.active_cursor = None
        
        # MySQL connector override for users who know it's installed
        self.mysql_override = tk.BooleanVar(value=False)
        
        # SQL operations variables
        self.order_by_columns = []
        self.group_by_columns = []
        self.aggregate_functions = {}
        self.where_conditions = []
        
        # Results data
        self.result_data = None
        
        self.create_widgets()
    
    def create_widgets(self):
        # Create notebook for tabs
        notebook = ttk.Notebook(self.root)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create tabs
        connection_tab = ttk.Frame(notebook)
        tables_tab = ttk.Frame(notebook)
        columns_tab = ttk.Frame(notebook)
        operations_tab = ttk.Frame(notebook)  # New tab for operations
        query_tab = ttk.Frame(notebook)
        results_tab = ttk.Frame(notebook)  # New tab for results
        
        notebook.add(connection_tab, text="1. Connect")
        notebook.add(tables_tab, text="2. Select Tables")
        notebook.add(columns_tab, text="3. Select Columns")
        notebook.add(operations_tab, text="4. Operations")
        notebook.add(query_tab, text="5. Generate Query")
        notebook.add(results_tab, text="6. Results")  # Added results tab
        
        # Connection tab
        self.setup_connection_tab(connection_tab)
        
        # Tables tab
        self.setup_tables_tab(tables_tab)
        
        # Columns tab
        self.setup_columns_tab(columns_tab)
        
        # Operations tab
        self.setup_operations_tab(operations_tab)
        
        # Query tab
        self.setup_query_tab(query_tab)
        
        # Results tab
        self.setup_results_tab(results_tab)
    
    def setup_connection_tab(self, tab):
        # Server frame
        server_frame = ttk.LabelFrame(tab, text="Database Connection")
        server_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Database type
        ttk.Label(server_frame, text="Database Type:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        db_combo = ttk.Combobox(server_frame, textvariable=self.db_type)
        db_combo['values'] = ('SQL Server', 'MySQL')
        db_combo.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        db_combo.bind("<<ComboboxSelected>>", self.toggle_db_fields)
        
        # MySQL connector override checkbox
        ttk.Checkbutton(
            server_frame, 
            text="I have MySQL connector installed (bypass check)", 
            variable=self.mysql_override
        ).grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)
        
        # Authentication type
        ttk.Label(server_frame, text="Authentication:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        auth_combo = ttk.Combobox(server_frame, textvariable=self.auth_type)
        auth_combo['values'] = ('Windows Authentication', 'SQL Server Authentication')
        auth_combo.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        auth_combo.bind("<<ComboboxSelected>>", self.toggle_auth)
        
        # Server
        ttk.Label(server_frame, text="Server:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(server_frame, textvariable=self.server, width=30).grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Port (for MySQL)
        ttk.Label(server_frame, text="Port:").grid(row=3, column=0, sticky=tk.W, padx=5, pady=5)
        self.port_entry = ttk.Entry(server_frame, textvariable=self.port, width=30)
        self.port_entry.grid(row=3, column=1, sticky=tk.W, padx=5, pady=5)
        self.port_entry.configure(state=tk.DISABLED)  # Disabled by default for SQL Server
        
        # Database
        ttk.Label(server_frame, text="Database:").grid(row=4, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(server_frame, textvariable=self.database, width=30).grid(row=4, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Username and password (disabled by default for Windows Auth)
        ttk.Label(server_frame, text="Username:").grid(row=5, column=0, sticky=tk.W, padx=5, pady=5)
        self.username_entry = ttk.Entry(server_frame, textvariable=self.username, width=30, state=tk.DISABLED)
        self.username_entry.grid(row=5, column=1, sticky=tk.W, padx=5, pady=5)
        
        ttk.Label(server_frame, text="Password:").grid(row=6, column=0, sticky=tk.W, padx=5, pady=5)
        self.password_entry = ttk.Entry(server_frame, textvariable=self.password, width=30, show="*", state=tk.DISABLED)
        self.password_entry.grid(row=6, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Connect button
        connect_btn = ttk.Button(server_frame, text="Connect", command=self.connect_to_database, style="Light.TButton")
        connect_btn.grid(row=7, column=0, columnspan=2, pady=20)
        self.create_tooltip(connect_btn, "Click to establish DB connection")

    def setup_tables_tab(self, tab):
        tables_frame = ttk.LabelFrame(tab, text="Select Tables")
        tables_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Table selection - Modified to allow more than 2 tables
        self.tables_listbox = tk.Listbox(tables_frame, selectmode=tk.MULTIPLE, height=15)
        self.tables_listbox.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        scrollbar = ttk.Scrollbar(self.tables_listbox, orient="vertical", command=self.tables_listbox.yview)
        self.tables_listbox.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Continue button
        ttk.Button(tab, text="Continue to Column Selection", command=self.get_table_columns).pack(pady=10)

    def setup_columns_tab(self, tab):
        # Create columns selection area with notebook for tables
        self.columns_notebook = ttk.Notebook(tab)
        self.columns_notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Continue button
        ttk.Button(tab, text="Continue to Operations", command=self.prepare_operations).pack(pady=10)
        
        # We'll setup the join configuration in a separate method when tables are selected
        self.join_frame = ttk.LabelFrame(tab, text="Join Configuration")
        self.join_frame.pack(fill=tk.BOTH, padx=10, pady=10)
        
        # Just a placeholder message initially
        ttk.Label(self.join_frame, text="Select tables first to configure joins").pack(padx=10, pady=10)
    
    def setup_operations_tab(self, tab):
        # Create notebook for different operations
        operations_notebook = ttk.Notebook(tab)
        operations_notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create different tabs for different operations
        order_by_tab = ttk.Frame(operations_notebook)
        group_by_tab = ttk.Frame(operations_notebook)
        aggregate_tab = ttk.Frame(operations_notebook)
        where_tab = ttk.Frame(operations_notebook)
        combined_columns_tab = ttk.Frame(operations_notebook)  # New tab for combined columns
        
        operations_notebook.add(order_by_tab, text="ORDER BY")
        operations_notebook.add(group_by_tab, text="GROUP BY")
        operations_notebook.add(aggregate_tab, text="Aggregate Functions")
        operations_notebook.add(where_tab, text="WHERE")
        operations_notebook.add(combined_columns_tab, text="Combined Columns")  # Add the new tab
        
        # Setup each operation tab
        self.setup_order_by_tab(order_by_tab)
        self.setup_group_by_tab(group_by_tab)
        self.setup_aggregate_tab(aggregate_tab)
        self.setup_where_tab(where_tab)
        self.setup_combined_columns_tab(combined_columns_tab)  # Setup new tab
        
        # Continue button
        ttk.Button(tab, text="Generate SQL", command=self.generate_sql).pack(pady=10)

    def setup_combined_columns_tab(self, tab):
        # Create frame for Combined Columns
        combined_frame = ttk.LabelFrame(tab, text="Combine Columns with Same Name")
        combined_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create a canvas with scrollbar for the combined columns
        canvas = tk.Canvas(combined_frame)
        scrollbar = ttk.Scrollbar(combined_frame, orient="vertical", command=canvas.yview)
        scroll_frame = ttk.Frame(canvas)
        
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        
        # Create list to hold combined column entries
        self.combined_column_entries = []
        
        # Info label explaining the feature
        info_text = ("This feature lets you combine columns from different tables into a single output column.\n"
                     "For example, if both tables have an 'id' column, you can merge them into one column.\n\n"
                     "You can create multiple combined columns - each will appear as a separate COALESCE function.")
        ttk.Label(scroll_frame, text=info_text, wraplength=500).pack(anchor=tk.W, padx=10, pady=5)
        
        # Add entries button - more prominent
        ttk.Button(
            scroll_frame, 
            text="Add New Combined Column", 
            command=lambda: self.add_combined_column_entry(scroll_frame, canvas),
            style="Light.TButton"
        ).pack(anchor=tk.CENTER, padx=10, pady=10)
        
        # Configure canvas scrolling
        scroll_frame.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
        canvas.bind_all("<MouseWheel>", lambda event, c=canvas: c.yview_scroll(int(-1*(event.delta/120)), "units"))

    def add_combined_column_entry(self, parent, canvas):
        frame = ttk.Frame(parent)
        frame.pack(anchor=tk.W, padx=10, pady=5, fill=tk.X)
        
        # Get all available columns - make a copy to ensure independence between entries
        all_columns = self.get_all_available_columns().copy()
        
        # Create a title for this combined column
        title_frame = ttk.Frame(frame)
        title_frame.grid(row=0, column=0, columnspan=3, sticky=tk.W)
        
        ttk.Label(title_frame, text="Combined Column", font=('Arial', 10, 'bold')).pack(side=tk.LEFT)
        
        # Alias for combined column - place at the top for better visibility
        alias_frame = ttk.Frame(frame)
        alias_frame.grid(row=1, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        ttk.Label(alias_frame, text="Output column alias:").pack(side=tk.LEFT)
        alias_var = tk.StringVar()
        alias_entry = ttk.Entry(alias_frame, textvariable=alias_var, width=20)
        alias_entry.pack(side=tk.LEFT, padx=5)
        
        # Dictionary to store checkbox variables
        checkbox_vars = {}
        
        # Select columns to combine - now using checkboxes
        columns_frame = ttk.Frame(frame)
        columns_frame.grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        
        ttk.Label(columns_frame, text="Select columns to combine:").pack(anchor=tk.W)
        
        # Create a scrollable frame for checkboxes
        check_canvas = tk.Canvas(columns_frame, height=150, width=550)
        check_scrollbar = ttk.Scrollbar(columns_frame, orient="vertical", command=check_canvas.yview)
        check_scroll_frame = ttk.Frame(check_canvas)
        
        check_canvas.configure(yscrollcommand=check_scrollbar.set)
        check_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        check_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        check_canvas.create_window((0, 0), window=check_scroll_frame, anchor="nw")
        
        # Create checkboxes for all columns
        for col in all_columns:
            var = tk.BooleanVar(value=False)
            checkbox_vars[col] = var
            
            cb_frame = ttk.Frame(check_scroll_frame)
            cb_frame.pack(anchor=tk.W, fill=tk.X)
            
            cb = ttk.Checkbutton(cb_frame, text=col, variable=var)
            cb.pack(side=tk.LEFT, padx=5, pady=2)
        
        # Update scrollable region
        check_scroll_frame.update_idletasks()
        check_canvas.config(scrollregion=check_canvas.bbox("all"))
        check_canvas.bind_all("<MouseWheel>", lambda event, c=check_canvas: c.yview_scroll(int(-1*(event.delta/120)), "units"))
        
        # Add button to suggest alias based on selected columns
        ttk.Button(
            alias_frame, 
            text="Suggest Alias", 
            command=lambda cv=checkbox_vars, av=alias_var: self.suggest_combined_alias_from_checkboxes(cv, av)
        ).pack(side=tk.LEFT, padx=5)
        
        # Optional filter for column names
        filter_frame = ttk.Frame(frame)
        filter_frame.grid(row=3, column=0, columnspan=3, sticky=tk.W, padx=5, pady=5)
        
        ttk.Label(filter_frame, text="Filter by column name:").pack(side=tk.LEFT)
        filter_var = tk.StringVar()
        filter_entry = ttk.Entry(filter_frame, textvariable=filter_var, width=20)
        filter_entry.pack(side=tk.LEFT, padx=5)
        
        # Create a separate filter function for checkboxes
        def filter_checkboxes(*args):
            search_term = filter_var.get().lower()
            for child in check_scroll_frame.winfo_children():
                # Each child is a frame containing a checkbox
                checkbox = child.winfo_children()[0]  # Get the checkbox inside the frame
                col_name = checkbox.cget("text")
                
                if search_term in col_name.lower():
                    child.pack(anchor=tk.W, fill=tk.X)  # Show
                else:
                    child.pack_forget()  # Hide
                    
            # Update canvas scroll region
            check_scroll_frame.update_idletasks()
            check_canvas.config(scrollregion=check_canvas.bbox("all"))
        
        # Connect filter function
        filter_var.trace('w', filter_checkboxes)
        
        # Put a visual separator to make entries more distinct
        separator = ttk.Separator(frame, orient="horizontal")
        separator.grid(row=4, column=0, columnspan=3, sticky=tk.EW, pady=5)
        
        # Remove button in a separate frame at the bottom
        btn_frame = ttk.Frame(frame)
        btn_frame.grid(row=5, column=0, columnspan=3, pady=5)
        
        ttk.Button(
            btn_frame, 
            text="Remove this Combined Column", 
            command=lambda f=frame, e={
                "checkbox_vars": checkbox_vars,
                "alias_var": alias_var,
                "all_columns": all_columns,
                "filter_var": filter_var,
                "check_canvas": check_canvas,
                "check_scroll_frame": check_scroll_frame
            }: self.remove_combined_column_entry(f, e, canvas)
        ).pack(pady=5)
        
        # Store complete entry data with the new checkbox vars
        entry_data = {
            "checkbox_vars": checkbox_vars,
            "alias_var": alias_var,
            "all_columns": all_columns,
            "filter_var": filter_var,
            "check_canvas": check_canvas,
            "check_scroll_frame": check_scroll_frame
        }
        self.combined_column_entries.append(entry_data)
        
        # Update canvas scroll region
        parent.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))

    def remove_combined_column_entry(self, frame, entry_data, canvas):
        self.combined_column_entries.remove(entry_data)
        frame.destroy()
        frame.master.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))

    def suggest_combined_alias(self, listbox, alias_var):
        selected_indices = listbox.curselection()
        if not selected_indices:
            return
        
        # Extract the common column name from the selected columns
        selected_columns = [listbox.get(i) for i in selected_indices]
        common_names = set()
        
        for col in selected_columns:
            if "." in col:
                # Extract just the column name without the table prefix
                column_name = col.split(".")[-1]
                common_names.add(column_name)
        
        if len(common_names) == 1:
            # If all selected columns share the same name, use that
            alias_var.set(list(common_names)[0])
        else:
            # Otherwise, create a combined name
            alias_var.set("combined_" + "_".join(common_names))

    def suggest_combined_alias_from_checkboxes(self, checkbox_vars, alias_var):
        """Suggest alias based on checked columns"""
        # Get selected columns from checkboxes
        selected_columns = [col for col, var in checkbox_vars.items() if var.get()]
        
        if not selected_columns:
            return
            
        common_names = set()
        for col in selected_columns:
            if "." in col:
                # Extract just the column name without the table prefix
                column_name = col.split(".", 1)[-1]
                common_names.add(column_name)
        
        if len(common_names) == 1:
            # If all selected columns share the same name, use that
            alias_var.set(list(common_names)[0])
        else:
            # Otherwise, create a combined name
            alias_var.set("combined_" + "_".join(common_names))

    def setup_order_by_tab(self, tab):
        # Create frame for ORDER BY selection
        order_by_frame = ttk.LabelFrame(tab, text="Order By Columns")
        order_by_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create a canvas with scrollbar for the columns
        canvas = tk.Canvas(order_by_frame)
        scrollbar = ttk.Scrollbar(order_by_frame, orient="vertical", command=canvas.yview)
        scroll_frame = ttk.Frame(canvas)
        
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        
        # Create list to hold order by entries
        self.order_by_entries = []
        
        # Add entries button
        ttk.Button(scroll_frame, text="Add Order By Column", command=lambda: self.add_order_by_entry(scroll_frame, canvas)).pack(anchor=tk.W, padx=10, pady=5)
        
        # Configure canvas scrolling
        scroll_frame.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
        canvas.bind_all("<MouseWheel>", lambda event, c=canvas: c.yview_scroll(int(-1*(event.delta/120)), "units"))
    
    def add_order_by_entry(self, parent, canvas):
        frame = ttk.Frame(parent)
        frame.pack(anchor=tk.W, padx=10, pady=5, fill=tk.X)
        
        # Get all available columns from selected tables
        all_columns = self.get_all_available_columns()
        
        # Column dropdown instead of entry - now wider and searchable
        ttk.Label(frame, text="Column:").pack(side=tk.LEFT)
        col_combo = self.create_searchable_combobox(frame, all_columns, width=40)
        col_combo.pack(side=tk.LEFT, padx=5)
        
        # Order combobox
        ttk.Label(frame, text="Order:").pack(side=tk.LEFT)
        order_combo = ttk.Combobox(frame, values=SORT_ORDERS, width=5)
        order_combo.current(0)  # Default to ASC
        order_combo.pack(side=tk.LEFT, padx=5)
        
        # Remove button
        ttk.Button(frame, text="Remove", command=lambda f=frame, e=(col_combo, order_combo): self.remove_order_by_entry(f, e, canvas)).pack(side=tk.LEFT, padx=5)
        
        # Add to entries list
        self.order_by_entries.append((col_combo, order_combo))
        
        # Update canvas scroll region
        parent.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
    
    # Helper method to get all columns from selected tables
    def get_all_available_columns(self):
        all_columns = []
        for table in self.selected_tables:
            if table in self.tables:
                for column in self.tables[table]:
                    all_columns.append(f"{table}.{column}")
        return all_columns
    
    # New method to create searchable comboboxes for column selection
    def create_searchable_combobox(self, parent, values, width=40):
        """Create a combobox with search-as-you-type functionality"""
        # Create a custom StringVar that will store both the visible text and filter the dropdown
        var = tk.StringVar()
        
        # Create the combobox with the specified width
        combo = ttk.Combobox(parent, textvariable=var, width=width)
        combo['values'] = values
        
        # Function to filter dropdown based on entered text
        def filter_dropdown(event=None):
            typed_text = var.get().lower()
            if typed_text:
                filtered_values = [v for v in values if typed_text in v.lower()]
                combo['values'] = filtered_values or values  # Show filtered or all if no matches
            else:
                combo['values'] = values  # Show all when no filter
        
        # Bind events for filtering
        var.trace('w', lambda name, index, mode: filter_dropdown())
        combo.bind('<KeyRelease>', filter_dropdown)
        
        return combo
    
    def remove_order_by_entry(self, frame, entry_tuple, canvas):
        self.order_by_entries.remove(entry_tuple)
        frame.destroy()
        
        # Update canvas scroll region
        frame.master.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
    
    def setup_group_by_tab(self, tab):
        # Create frame for GROUP BY selection
        group_by_frame = ttk.LabelFrame(tab, text="Group By Columns")
        group_by_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create a canvas with scrollbar for the columns
        canvas = tk.Canvas(group_by_frame)
        scrollbar = ttk.Scrollbar(group_by_frame, orient="vertical", command=canvas.yview)
        scroll_frame = ttk.Frame(canvas)
        
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        
        # Create list to hold group by entries
        self.group_by_entries = []
        
        # Add entries button
        ttk.Button(scroll_frame, text="Add Group By Column", command=lambda: self.add_group_by_entry(scroll_frame, canvas)).pack(anchor=tk.W, padx=10, pady=5)
        
        # Configure canvas scrolling
        scroll_frame.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
        canvas.bind_all("<MouseWheel>", lambda event, c=canvas: c.yview_scroll(int(-1*(event.delta/120)), "units"))
    
    def add_group_by_entry(self, parent, canvas):
        frame = ttk.Frame(parent)
        frame.pack(anchor=tk.W, padx=10, pady=5, fill=tk.X)
        
        # Get all available columns
        all_columns = self.get_all_available_columns()
        
        # Column dropdown instead of entry - now wider and searchable
        ttk.Label(frame, text="Column:").pack(side=tk.LEFT)
        col_combo = self.create_searchable_combobox(frame, all_columns, width=40)
        col_combo.pack(side=tk.LEFT, padx=5)
        
        # Remove button
        ttk.Button(frame, text="Remove", command=lambda f=frame, c=col_combo: self.remove_group_by_entry(f, c, canvas)).pack(side=tk.LEFT, padx=5)
        
        # Add to entries list
        self.group_by_entries.append(col_combo)
        
        # Update canvas scroll region
        parent.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
    
    def remove_group_by_entry(self, frame, entry, canvas):
        self.group_by_entries.remove(entry)
        frame.destroy()
        
        # Update canvas scroll region
        frame.master.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
    
    def setup_aggregate_tab(self, tab):
        # Create frame for Aggregate Functions
        agg_frame = ttk.LabelFrame(tab, text="Aggregate Functions")
        agg_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create a canvas with scrollbar for the aggregate functions
        canvas = tk.Canvas(agg_frame)
        scrollbar = ttk.Scrollbar(agg_frame, orient="vertical", command=canvas.yview)
        scroll_frame = ttk.Frame(canvas)
        
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        
        # Create list to hold aggregate function entries
        self.aggregate_entries = []
        
        # Add entries button
        ttk.Button(scroll_frame, text="Add Aggregate Function", command=lambda: self.add_aggregate_entry(scroll_frame, canvas)).pack(anchor=tk.W, padx=10, pady=5)
        
        # Configure canvas scrolling
        scroll_frame.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
        canvas.bind_all("<MouseWheel>", lambda event, c=canvas: c.yview_scroll(int(-1*(event.delta/120)), "units"))
    
    def add_aggregate_entry(self, parent, canvas):
        frame = ttk.Frame(parent)
        frame.pack(anchor=tk.W, padx=10, pady=5, fill=tk.X)
        
        # Get all available columns
        all_columns = self.get_all_available_columns()
        
        # Function combo
        ttk.Label(frame, text="Function:").pack(side=tk.LEFT)
        func_combo = ttk.Combobox(frame, values=AGGREGATE_FUNCTIONS, width=10)
        func_combo.current(0)  # Default to COUNT
        func_combo.pack(side=tk.LEFT, padx=5)
        
        # Column dropdown instead of entry - now wider and searchable
        ttk.Label(frame, text="Column:").pack(side=tk.LEFT)
        col_combo = self.create_searchable_combobox(frame, all_columns, width=40)
        col_combo.pack(side=tk.LEFT, padx=5)
        
        # Alias entry with suggestion
        ttk.Label(frame, text="AS:").pack(side=tk.LEFT)
        alias_entry = ttk.Entry(frame, width=15)
        alias_entry.pack(side=tk.LEFT, padx=5)
        
        # Auto-generate alias suggestion based on function and column selection
        def update_alias_suggestion(*args):
            try:
                func = func_combo.get()
                col = col_combo.get()
                if func and col:
                    # Extract column name without table prefix
                    if "." in col:
                        table_prefix, col_name = col.split(".", 1)
                    else:
                        col_name = col
                    
                    # Generate a suggested alias - combine function name with column name
                    suggested_alias = f"{func}{col_name}"
                    
                    # Only set if alias is empty
                    if not alias_entry.get():
                        alias_entry.delete(0, tk.END)
                        alias_entry.insert(0, suggested_alias)
                    
                    # When selecting a column for aggregation, optionally offer to deselect the original column
                    if col in self.get_all_available_columns():
                        # Check if the column is currently selected in the columns tab
                        if "." in col:
                            table_name, col_name = col.split(".", 1)
                            if table_name in self.selected_columns and col_name in self.selected_columns[table_name]:
                                if messagebox.askyesno("Remove Duplicate", 
                                                      f"Would you like to remove {col} from the regular columns selection?\n\n"
                                                      f"This avoids duplicating the same data in your query results."):
                                    # Deselect the original column
                                    self.selected_columns[table_name][col_name].set(False)
            except Exception as e:
                print(f"Error in alias suggestion: {str(e)}")
        
        # Bind events to update alias suggestion
        func_combo.bind("<<ComboboxSelected>>", update_alias_suggestion)
        col_combo.bind("<<ComboboxSelected>>", update_alias_suggestion)
        
        # Remove button
        ttk.Button(frame, text="Remove", command=lambda f=frame, e=(func_combo, col_combo, alias_entry): self.remove_aggregate_entry(f, e, canvas)).pack(side=tk.LEFT, padx=5)
        
        # Add to entries list
        self.aggregate_entries.append((func_combo, col_combo, alias_entry))
        
        # Update canvas scroll region
        parent.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
    
    def remove_aggregate_entry(self, frame, entry_tuple, canvas):
        self.aggregate_entries.remove(entry_tuple)
        frame.destroy()
        
        # Update canvas scroll region
        frame.master.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
    
    def setup_where_tab(self, tab):
        # Create frame for WHERE conditions
        where_frame = ttk.LabelFrame(tab, text="WHERE Conditions")
        where_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create a canvas with scrollbar for the where conditions
        canvas = tk.Canvas(where_frame)
        scrollbar = ttk.Scrollbar(where_frame, orient="vertical", command=canvas.yview)
        scroll_frame = ttk.Frame(canvas)
        
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        
        # Create list to hold where condition entries
        self.where_entries = []
        
        # Add entries button
        ttk.Button(scroll_frame, text="Add Where Condition", command=lambda: self.add_where_entry(scroll_frame, canvas)).pack(anchor=tk.W, padx=10, pady=5)
        
        # Configure canvas scrolling
        scroll_frame.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
        canvas.bind_all("<MouseWheel>", lambda event, c=canvas: c.yview_scroll(int(-1*(event.delta/120)), "units"))
    
    def add_where_entry(self, parent, canvas):
        frame = ttk.Frame(parent)
        frame.pack(anchor=tk.W, padx=10, pady=5, fill=tk.X)
        
        # Get all available columns
        all_columns = self.get_all_available_columns()
        
        # Column dropdown instead of entry - now wider and searchable
        ttk.Label(frame, text="Column:").pack(side=tk.LEFT)
        col_combo = self.create_searchable_combobox(frame, all_columns, width=40)
        col_combo.pack(side=tk.LEFT, padx=5)
        
        # Operator combo
        ttk.Label(frame, text="Operator:").pack(side=tk.LEFT)
        op_combo = ttk.Combobox(frame, values=["=", ">", "<", ">=", "<=", "<>", "LIKE", "IN", "IS NULL", "IS NOT NULL"], width=8)
        op_combo.current(0)  # Default to =
        op_combo.pack(side=tk.LEFT, padx=5)
        
        # Value entry
        ttk.Label(frame, text="Value:").pack(side=tk.LEFT)
        val_entry = ttk.Entry(frame, width=15)
        val_entry.pack(side=tk.LEFT, padx=5)
        
        # Connector combo (AND/OR)
        conn_combo = ttk.Combobox(frame, values=["AND", "OR"], width=5)
        conn_combo.current(0)  # Default to AND
        conn_combo.pack(side=tk.LEFT, padx=5)
        
        # Remove button
        ttk.Button(frame, text="Remove", command=lambda f=frame, e=(col_combo, op_combo, val_entry, conn_combo): self.remove_where_entry(f, e, canvas)).pack(side=tk.LEFT, padx=5)
        
        # Add to entries list
        self.where_entries.append((col_combo, op_combo, val_entry, conn_combo))
        
        # Update canvas scroll region
        parent.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
    
    def remove_where_entry(self, frame, entry_tuple, canvas):
        self.where_entries.remove(entry_tuple)
        frame.destroy()
        
        # Update canvas scroll region
        frame.master.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
    
    def prepare_operations(self):
        # This method prepares the operations tab for use
        # Make sure tables are selected before operations can be used
        if not self.selected_tables:
            messagebox.showwarning("No Tables Selected", "Please select tables first.")
            return
    
    def toggle_db_fields(self, event):
        if self.db_type.get() == "MySQL":
            self.port_entry.configure(state=tk.NORMAL)
            self.auth_type.set("SQL Server Authentication")  # MySQL always uses username/password
            self.username_entry.configure(state=tk.NORMAL)
            self.password_entry.configure(state=tk.NORMAL)
        else:
            self.port_entry.configure(state=tk.DISABLED)
            if self.auth_type.get() == "Windows Authentication":
                self.username_entry.configure(state=tk.DISABLED)
                self.password_entry.configure(state=tk.DISABLED)
            else:
                self.username_entry.configure(state=tk.NORMAL)
                self.password_entry.configure(state=tk.NORMAL)

    def toggle_auth(self, event):
        if self.db_type.get() == "MySQL":
            # MySQL always uses username/password authentication
            self.username_entry.config(state=tk.NORMAL)
            self.password_entry.config(state=tk.NORMAL)
        else:
            # SQL Server can use Windows or SQL Server authentication
            if self.auth_type.get() == "Windows Authentication":
                self.username_entry.config(state=tk.DISABLED)
                self.password_entry.config(state=tk.DISABLED)
            else:
                self.username_entry.config(state=tk.NORMAL)
                self.password_entry.config(state=tk.NORMAL)
    
    def connect_to_database(self):
        try:
            if self.db_type.get() == "MySQL":
                # Check if MySQL connector is available or if override is checked
                global mysql_connector
                
                if not mysql_available and self.mysql_override.get():
                    # Try to import it again if override is checked
                    try:
                        import mysql.connector as mysql_connector_override
                        mysql_connector = mysql_connector_override
                    except ImportError:
                        messagebox.showerror(
                            "Module Not Found",
                            "Could not import MySQL connector module even with override.\n"
                            "Please install it using: pip install mysql-connector-python"
                        )
                        return
                elif not mysql_available and not self.mysql_override.get():
                    messagebox.showerror(
                        "Missing Module", 
                        "The MySQL connector module could not be detected.\n\n"
                        "If you already installed it, check 'I have MySQL connector installed' box.\n\n"
                        "Otherwise, install it using: pip install mysql-connector-python"
                    )
                    return
                
                try:
                    # Connect to MySQL database using the available connector
                    conn = mysql_connector.connect(
                        host=self.server.get(),
                        port=int(self.port.get()),  # Convert port to integer
                        user=self.username.get(),
                        password=self.password.get(),
                        database=self.database.get()
                    )
                    
                    cursor = conn.cursor()
                    
                    # Store active connection for later data fetching
                    self.active_conn = conn
                    self.active_cursor = cursor
                    
                    # Get tables
                    cursor.execute("SHOW TABLES")
                    tables = cursor.fetchall()
                    
                    # Clear tables dictionary and listbox
                    self.tables = {}
                    self.tables_listbox.delete(0, tk.END)
                    
                    # Add tables to listbox
                    for table in tables:
                        self.tables_listbox.insert(tk.END, table[0])
                    
                    # Get column info for each table
                    for table_name in [table[0] for table in tables]:
                        cursor.execute(f"SHOW COLUMNS FROM `{table_name}`")
                        columns = cursor.fetchall()
                        self.tables[table_name] = [col[0] for col in columns]
                    
                    # Don't close connection - keep it open for data fetching
                    messagebox.showinfo("Success", f"Connected to MySQL database {self.database.get()} successfully.\nFound {len(self.tables)} tables.")
                    
                except Exception as e:
                    messagebox.showerror("MySQL Connection Failed", str(e))
                    
            else:
                # SQL Server connection
                # Build connection string
                if self.auth_type.get() == "Windows Authentication":
                    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server.get()};DATABASE={self.database.get()};Trusted_Connection=yes;"
                else:
                    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.server.get()};DATABASE={self.database.get()};UID={self.username.get()};PWD={self.password.get()}"
                
                # Connect to database
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
                
                # Store active connection for later data fetching
                self.active_conn = conn
                self.active_cursor = cursor
                
                # Get tables
                cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = ?", (self.database.get(),))
                tables = cursor.fetchall()
                
                # Clear tables dictionary and listbox
                self.tables = {}
                self.tables_listbox.delete(0, tk.END)
                
                # Add tables to listbox
                for table in tables:
                    self.tables_listbox.insert(tk.END, table[0])
                
                # Get column info for each table
                for table_name in [table[0] for table in tables]:
                    cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", (table_name,))
                    columns = cursor.fetchall()
                    self.tables[table_name] = [col[0] for col in columns]
                
                # Don't close connection - keep it open for data fetching
                messagebox.showinfo("Success", f"Connected to SQL Server database {self.database.get()} successfully.\nFound {len(self.tables)} tables.")
            
        except Exception as e:
            messagebox.showerror("Connection Failed", f"Unexpected error: {str(e)}")
    
    def load_from_csv(self, table_identifier):
        import tkinter.filedialog as filedialog
        
        file_path = filedialog.askopenfilename(filetypes=[("CSV files", "*.csv")])
        if not file_path:
            return
        
        try:
            df = pd.read_csv(file_path)
            table_name = f"Table{table_identifier}"
            self.tables[table_name] = list(df.columns)
            
            # Update tables list
            if table_name not in [self.tables_listbox.get(i) for i in range(self.tables_listbox.size())]:
                self.tables_listbox.insert(tk.END, table_name)
            
            messagebox.showinfo("Success", f"Loaded {table_name} with {len(df.columns)} columns")
        except Exception as e:
            messagebox.showerror("Error loading CSV", str(e))
    
    def add_test_tables(self):
        table_a_name = self.table_a.get()
        table_b_name = self.table_b.get()
        table_c_name = self.table_c.get()
        
        # Generate sample columns for each table
        self.tables[table_a_name] = [f"A_Column_{i}" for i in range(1, 301)]
        self.tables[table_b_name] = [f"B_Column_{i}" for i in range(1, 251)]
        self.tables[table_c_name] = [f"C_Column_{i}" for i in range(1, 201)]
        
        # Clear listbox and add new tables
        self.tables_listbox.delete(0, tk.END)
        self.tables_listbox.insert(tk.END, table_a_name)
        self.tables_listbox.insert(tk.END, table_b_name)
        self.tables_listbox.insert(tk.END, table_c_name)
        
        messagebox.showinfo("Test Data", f"Added test tables:\n{table_a_name} (300 columns)\n{table_b_name} (250 columns)\n{table_c_name} (200 columns)")
    
    def get_table_columns(self):
        # Get selected tables
        selected_indices = self.tables_listbox.curselection()
        if len(selected_indices) < 1:
            messagebox.showwarning("Table Selection", "Please select at least one table.")
            return
        
        self.selected_tables = [self.tables_listbox.get(idx) for idx in selected_indices]
        
        # Clear notebook tabs
        for tab in self.columns_notebook.tabs():
            self.columns_notebook.forget(tab)
        
        # Unbind any existing mousewheel events
        try:
            self.root.unbind_all("<MouseWheel>")
        except:
            pass
        
        # Dictionary to store all canvas objects for proper scrolling
        self.column_canvases = {}
        self.checkbox_widgets_dict = {}
        self.search_vars = {}  # Store search StringVars for each table
        
        # Populate notebook with tables and their columns
        for table_name in self.selected_tables:
            if table_name in self.tables:
                # Create tab for table
                tab = ttk.Frame(self.columns_notebook)
                self.columns_notebook.add(tab, text=table_name)
                
                # Frame with scrollbar for checkboxes
                columns_frame = ttk.Frame(tab)
                columns_frame.pack(fill=tk.BOTH, expand=True)
                
                canvas = tk.Canvas(columns_frame)
                scrollbar = ttk.Scrollbar(columns_frame, orient="vertical", command=canvas.yview)
                checkboxes_frame = ttk.Frame(canvas)
                
                canvas.configure(yscrollcommand=scrollbar.set)
                scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
                canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
                canvas.create_window((0, 0), window=checkboxes_frame, anchor="nw")
                
                # Store canvas for later scrolling
                self.column_canvases[table_name] = canvas
                
                # Add search bar
                search_frame = ttk.Frame(tab)
                search_frame.pack(fill=tk.X, padx=10, pady=5)
                
                ttk.Label(search_frame, text="Search:").pack(side=tk.LEFT, padx=5)
                search_var = tk.StringVar(name=f"search_{table_name}")  # Unique name for each StringVar
                self.search_vars[table_name] = search_var  # Store reference
                search_entry = ttk.Entry(search_frame, textvariable=search_var, width=30)
                search_entry.pack(side=tk.LEFT, padx=5)
                
                # Initialize column selection dictionary
                if table_name not in self.selected_columns:
                    self.selected_columns[table_name] = {}
                
                # Select/deselect all buttons
                buttons_frame = ttk.Frame(tab)
                buttons_frame.pack(fill=tk.X, padx=10, pady=5)
                
                ttk.Button(buttons_frame, text="Select All", 
                           command=lambda t=table_name, cf=checkboxes_frame: self.select_all_columns(t, cf, True)).pack(side=tk.LEFT, padx=5)
                ttk.Button(buttons_frame, text="Deselect All", 
                           command=lambda t=table_name, cf=checkboxes_frame: self.select_all_columns(t, cf, False)).pack(side=tk.LEFT, padx=5)
                
                # Variable to store checkbox widgets
                checkbox_widgets = []
                
                # Add checkboxes for columns with column name labels
                for i, column in enumerate(self.tables[table_name]):
                    var = tk.BooleanVar(value=False)
                    self.selected_columns[table_name][column] = var
                    
                    frame = ttk.Frame(checkboxes_frame)
                    frame.pack(anchor=tk.W, padx=10, pady=2)
                    
                    cb = ttk.Checkbutton(frame, text=column, variable=var)
                    cb.pack(side=tk.LEFT)
                    checkbox_widgets.append((cb, column, frame))
                
                # Store the checkbox widgets for this table
                self.checkbox_widgets_dict[table_name] = checkbox_widgets
                
                # Configure scrolling
                checkboxes_frame.update_idletasks()
                canvas.config(scrollregion=canvas.bbox("all"))
                
        # Now set up search functionality after all tables are created
        for table_name in self.selected_tables:
            if table_name in self.search_vars and table_name in self.checkbox_widgets_dict:
                # Create a search function specific to this table
                self.setup_search_for_table(table_name, checkboxes_frame, canvas)

        # Set up mousewheel scrolling for the active tab
        def on_tab_change(event):
            try:
                current_tab = self.columns_notebook.select()
                table_name = self.columns_notebook.tab(current_tab, "text")
                
                # Unbind previous mousewheel events
                self.root.unbind_all("<MouseWheel>")
                
                # Only bind if the table exists in our canvas dictionary
                if (table_name in self.column_canvases):
                    # Bind mousewheel to the current canvas
                    canvas = self.column_canvases[table_name]
                    self.root.bind_all("<MouseWheel>", lambda e, c=canvas: c.yview_scroll(int(-1*(e.delta/120)), "units"))
            except:
                pass
        
        # Bind the tab change event
        self.columns_notebook.bind("<<NotebookTabChanged>>", on_tab_change)
        
        # Trigger once to set up the first tab
        if self.columns_notebook.tabs():
            # Get the first tab's table name
            first_tab = self.columns_notebook.tabs()[0]
            table_name = self.columns_notebook.tab(first_tab, "text")
            
            # Setup scrolling for the first tab
            if table_name in self.column_canvases:
                canvas = self.column_canvases[table_name]
                self.root.bind_all("<MouseWheel>", lambda e, c=canvas: c.yview_scroll(int(-1*(e.delta/120)), "units"))
        
        # Setup the join tab if more than one table is selected
        if len(self.selected_tables) > 1:
            self.setup_join_configuration()
            
    def setup_search_for_table(self, table_name, checkboxes_frame, canvas):
        """Set up search functionality for a specific table"""
        search_var = self.search_vars[table_name]
        widgets = self.checkbox_widgets_dict[table_name]
        can = self.column_canvases[table_name]
        
        # Define search function for this specific table
        def search_columns(*args):
            search_term = search_var.get().lower()
            for _, column_name, frame in widgets:
                if search_term in column_name.lower():
                    frame.pack(anchor=tk.W, padx=10, pady=2)
                else:
                    frame.pack_forget()
            # Update canvas scroll region after filtering
            can.update_idletasks()
            can.config(scrollregion=can.bbox("all"))
        
        # Create a unique callback ID for this table
        callback_name = f"search_callback_{table_name}"
        
        # Remove any existing trace to avoid duplicate callbacks
        try:
            search_var.trace_vdelete("w", search_var.trace_info()[0][1])
        except:
            pass
        
        # Add the trace with the new function
        search_var.trace("w", search_columns)
    
    def select_all_columns(self, table_name, checkboxes_frame, select_all):
        """Select or deselect all columns for a table"""
        for column, var in self.selected_columns[table_name].items():
            var.set(select_all)

    def setup_join_configuration(self):
        # Clear existing widgets in join frame
        for widget in self.join_frame.winfo_children():
            widget.destroy()
            
        # Create a canvas with scrollbar for the join conditions
        canvas = tk.Canvas(self.join_frame)
        scrollbar = ttk.Scrollbar(self.join_frame, orient="vertical", command=canvas.yview)
        scroll_frame = ttk.Frame(canvas)
        
        canvas.configure(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        canvas.create_window((0, 0), window=scroll_frame, anchor="nw")
        
        # Create list to hold join entries
        self.join_entries = []
        
        # Add a join entry for each pair of tables needed
        # For n tables, we need at least n-1 joins
        for i in range(len(self.selected_tables) - 1):
            # For the first join, use tables[0] and tables[1]
            # For subsequent joins, use the result of previous joins and the next table
            left_table = self.selected_tables[0] if i == 0 else None  # "result so far"
            right_table = self.selected_tables[i+1]
            
            self.add_join_entry(scroll_frame, canvas, left_table, right_table, i)
        
        # Configure canvas scrolling
        scroll_frame.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))
        canvas.bind_all("<MouseWheel>", lambda event, c=canvas: c.yview_scroll(int(-1*(event.delta/120)), "units"))

        # Store references so we can add more joins later
        self.join_scroll_frame = scroll_frame
        self.join_canvas = canvas

        # Add a button to add additional join configurations
        ttk.Button(scroll_frame, text="Add Another Join", command=self.add_another_join).pack(pady=10)
    
    def add_join_entry(self, parent, canvas, left_table=None, right_table=None, join_index=0):
        frame = ttk.LabelFrame(parent, text=f"Join {join_index + 1}")
        frame.pack(fill=tk.X, padx=10, pady=5)
        
        # Get all available columns
        all_left_columns = []
        all_right_columns = []
        
        # If this is the first join, allow selecting left table
        # Otherwise, left table is the result of previous joins
        if join_index == 0:
            # Left table selection
            ttk.Label(frame, text="Left Table:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
            left_combo = ttk.Combobox(frame, values=self.selected_tables, width=20)
            if left_table:
                left_combo.set(left_table)
                # Get columns for left table
                if left_table in self.tables:
                    all_left_columns = self.tables[left_table]
            left_combo.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
            
            # Update columns when left table changes
            def update_left_columns(event):
                selected_table = left_combo.get()
                if selected_table in self.tables:
                    new_columns = self.tables[selected_table]
                    left_col_combo['values'] = new_columns
            
            left_combo.bind("<<ComboboxSelected>>", update_left_columns)
        else:
            ttk.Label(frame, text="Left Table:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
            ttk.Label(frame, text="(Result of previous joins)").grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
            left_combo = None
            # For subsequent joins, all columns from previously selected tables are available
            for t in self.selected_tables[:join_index+1]:
                if t in self.tables:
                    all_left_columns.extend(self.tables[t])
        
        # Right table selection
        ttk.Label(frame, text="Right Table:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        right_combo = ttk.Combobox(frame, values=self.selected_tables, width=20)
        if right_table:
            right_combo.set(right_table)
            # Get columns for right table
            if right_table in self.tables:
                all_right_columns = self.tables[right_table]
        right_combo.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Update columns when right table changes
        def update_right_columns(event):
            selected_table = right_combo.get()
            if selected_table in self.tables:
                new_columns = self.tables[selected_table]
                right_col_combo['values'] = new_columns
        
        right_combo.bind("<<ComboboxSelected>>", update_right_columns)
        
        # Join type selection
        ttk.Label(frame, text="Join Type:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        join_values = [item["value"] for item in JOIN_TYPES]
        join_type_combo = ttk.Combobox(frame, values=join_values, width=20)
        join_type_combo.current(0)  # Default to INNER JOIN
        join_type_combo.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Left column for join - now searchable
        ttk.Label(frame, text="Left Column:").grid(row=3, column=0, sticky=tk.W, padx=5, pady=5)
        left_col_combo = self.create_searchable_combobox(frame, all_left_columns, width=40)
        left_col_combo.grid(row=3, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Right column for join - now searchable
        ttk.Label(frame, text="Right Column:").grid(row=4, column=0, sticky=tk.W, padx=5, pady=5)
        right_col_combo = self.create_searchable_combobox(frame, all_right_columns, width=40)
        right_col_combo.grid(row=4, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Store the join entry components
        self.join_entries.append((left_combo, right_combo, join_type_combo, left_col_combo, right_col_combo))
        
        # Remove button
        ttk.Button(
            frame, 
            text="Remove", 
            command=lambda f=frame, je=(left_combo, right_combo, join_type_combo, left_col_combo, right_col_combo): self.remove_join_entry(f, je, canvas)
        ).grid(row=5, column=0, columnspan=2, pady=5)
        
        # Update canvas scroll region
        parent.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))

    def remove_join_entry(self, frame, join_tuple, canvas):
        self.join_entries.remove(join_tuple)
        frame.destroy()
        frame.master.update_idletasks()
        canvas.config(scrollregion=canvas.bbox("all"))

    def add_another_join(self):
        # Let the user add an extra join configuration
        if len(self.selected_tables) < 2:
            messagebox.showwarning("Join Configuration", "Select at least two tables before adding more joins.")
            return
        # Use the next join index
        join_index = len(self.join_entries)
        self.add_join_entry(self.join_scroll_frame, self.join_canvas, None, None, join_index)

    def generate_sql(self):
        if not self.selected_tables:
            messagebox.showwarning("Selection Error", "Please select at least one table")
            return
        
        # Get all selected columns
        selected_columns_dict = {}
        for table in self.selected_tables:
            selected_columns_dict[table] = []
            for column, var in self.selected_columns[table].items():
                if var.get():
                    selected_columns_dict[table].append(column)
        
        # Generate SQL
        sql = "SELECT \n"
        
        # Track which columns are used in aggregates to avoid duplication
        aggregated_columns = set()
        
        # Collect all aggregate functions first to identify duplicated columns
        for func_combo, col_combo, alias_entry in self.aggregate_entries:
            col = col_combo.get()
            if col:
                aggregated_columns.add(col)
        
        # Add columns
        all_columns = []
        
        # Add regular selected columns (except those used in aggregates)
        for table in self.selected_tables:
            for column in selected_columns_dict[table]:
                column_full_name = f"{table}.{column}"
                # Only add if not used in an aggregate function
                if column_full_name not in aggregated_columns:
                    # Handle column names based on database type
                    if self.db_type.get() == "MySQL":
                        all_columns.append(f"`{table}`.`{column}`")
                    else:
                        all_columns.append(f"{table}.{column}")
        
        # Add combined column entries - update to handle checkbox vars
        for entry_data in self.combined_column_entries:
            checkbox_vars = entry_data["checkbox_vars"]
            alias_var = entry_data["alias_var"]
            
            # Get selected columns from checkboxes
            selected_columns = [col for col, var in checkbox_vars.items() if var.get()]
            
            if not selected_columns:
                continue
                
            alias = alias_var.get()
            if not alias:
                continue
                
            # Format the COALESCE function based on database type
            if self.db_type.get() == "MySQL":
                formatted_cols = []
                for col in selected_columns:
                    if "." in col:
                        table_name, col_name = col.split(".", 1)
                        formatted_cols.append(f"`{table_name}`.`{col_name}`")
                    else:
                        formatted_cols.append(f"`{col}`")
                coalesce_col = f"COALESCE({', '.join(formatted_cols)}) AS `{alias}`"
            else:
                coalesce_col = f"COALESCE({', '.join(selected_columns)}) AS {alias}"
                    
            all_columns.append(coalesce_col)
                
            # Mark these columns as used to avoid duplication
            for col in selected_columns:
                aggregated_columns.add(col)
        
        # Add aggregate functions
        for func_combo, col_combo, alias_entry in self.aggregate_entries:
            func = func_combo.get()
            col = col_combo.get()
            alias = alias_entry.get()
            
            if not col:
                continue
            
            # Format the aggregate function
            if self.db_type.get() == "MySQL":
                # Extract the column name without table prefix for MySQL if it has table prefix
                if "." in col:
                    table_name, col_name = col.split(".", 1)
                    agg_col = f"{func}(`{table_name}`.`{col_name}`)"
                else:
                    agg_col = f"{func}(`{col}`)"
            else:
                agg_col = f"{func}({col})"
            
            # Add alias if provided
            if alias:
                # Replace any dots in alias with underscores to avoid SQL syntax errors
                sanitized_alias = alias.replace(".", "_")
                agg_col += f" AS {sanitized_alias}"
            
            all_columns.append(agg_col)
        
        if not all_columns:
            messagebox.showwarning("No Columns", "Please select at least one column, combined column, or aggregate function")
            return
        
        sql += "    " + ",\n    ".join(all_columns)
        
        # Add FROM clause with first table
        if self.db_type.get() == "MySQL":
            sql += f"\nFROM `{self.selected_tables[0]}`"
        else:
            sql += f"\nFROM {self.selected_tables[0]}"
        
        # Add JOINs if multiple tables
        if len(self.selected_tables) > 1 and hasattr(self, 'join_entries'):
            for i, (left_combo, right_combo, join_type_combo, left_col_entry, right_col_entry) in enumerate(self.join_entries):
                join_type = join_type_combo.get()
                left_col = left_col_entry.get()
                right_col = right_col_entry.get()
                
                # For first join, get the left table from combo
                # For subsequent joins, left table is implicit (result of previous joins)
                if i == 0:
                    left_table = left_combo.get() if left_combo else self.selected_tables[0]
                else:
                    left_table = None  # Not needed for SQL generation after first join
                
                right_table = right_combo.get()
                
                if not right_table or not left_col or not right_col:
                    messagebox.showwarning("Join Error", f"Please complete all fields for Join {i+1}")
                    return
                
                # Format the join based on database type
                if self.db_type.get() == "MySQL":
                    if left_table:
                        sql += f"\n{join_type} `{right_table}` ON `{left_table}`.`{left_col}` = `{right_table}`.`{right_col}`"
                    else:
                        sql += f"\n{join_type} `{right_table}` ON `{right_table}`.`{right_col}` = `{left_col}`"
                else:
                    if left_table:
                        sql += f"\n{join_type} {right_table} ON {left_table}.{left_col} = {right_table}.{right_col}"
                    else:
                        sql += f"\n{join_type} {right_table} ON {right_table}.{right_col} = {left_col}"
        
        # Add WHERE clause if conditions exist
        if self.where_entries:
            sql += "\nWHERE "
            conditions = []
            
            for i, (col_combo, op_combo, val_entry, conn_combo) in enumerate(self.where_entries):
                col = col_combo.get()
                op = op_combo.get()
                val = val_entry.get()
                
                if not col or (op not in ["IS NULL", "IS NOT NULL"] and not val):
                    continue
                
                # Format the condition based on operator and database type
                if self.db_type.get() == "MySQL" and "." in col:
                    # Add proper backticks for MySQL mode
                    table_name, col_name = col.split(".", 1)
                    formatted_col = f"`{table_name}`.`{col_name}`"
                else:
                    formatted_col = col
                
                # Format the condition based on operator
                if op in ["IS NULL", "IS NOT NULL"]:
                    condition = f"{formatted_col} {op}"
                elif op == "LIKE":
                    condition = f"{formatted_col} LIKE '{val}'"
                elif op == "IN":
                    condition = f"{formatted_col} IN ({val})"
                else:
                    # Check if value is numeric
                    try:
                        float(val)
                        condition = f"{formatted_col} {op} {val}"
                    except ValueError:
                        condition = f"{formatted_col} {op} '{val}'"
                
                conditions.append(condition)
                
                # Add connector (AND/OR) except for the last condition
                if i < len(self.where_entries) - 1:
                    conditions.append(conn_combo.get())
            
            sql += " ".join(conditions)
        
        # Add GROUP BY if grouping columns exist
        if self.group_by_entries:
            group_cols = []
            for combo in self.group_by_entries:
                col = combo.get()
                if col:
                    # Format GROUP BY columns with proper backticks in MySQL mode
                    if self.db_type.get() == "MySQL" and "." in col:
                        table_name, col_name = col.split(".", 1)
                        group_cols.append(f"`{table_name}`.`{col_name}`")
                    else:
                        group_cols.append(col)
            
            if group_cols:
                sql += "\nGROUP BY " + ", ".join(group_cols)
        
        # Add ORDER BY if ordering columns exist
        if self.order_by_entries:
            order_cols = []
            for col_combo, order_combo in self.order_by_entries:
                col = col_combo.get()
                order = order_combo.get()
                if col:
                    # Format ORDER BY columns with proper backticks in MySQL mode
                    if self.db_type.get() == "MySQL" and "." in col:
                        table_name, col_name = col.split(".", 1)
                        order_cols.append(f"`{table_name}`.`{col_name}` {order}")
                    else:
                        order_cols.append(f"{col} {order}")
            
            if order_cols:
                sql += "\nORDER BY " + ", ".join(order_cols)
        
        # End query
        sql += ";"
        
        # Display in query tab
        self.query_text.delete(1.0, tk.END)
        self.query_text.insert(tk.END, sql)
    
    def copy_to_clipboard(self):
        sql = self.query_text.get(1.0, tk.END)
        self.root.clipboard_clear()
        self.root.clipboard_append(sql)
        messagebox.showinfo("Success", "SQL query copied to clipboard!")

    def setup_query_tab(self, tab):
        # Query display area
        query_frame = ttk.LabelFrame(tab, text="Generated SQL Query")
        query_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.query_text = scrolledtext.ScrolledText(query_frame, wrap=tk.WORD, width=80, height=15)
        self.query_text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Button frame
        button_frame = ttk.Frame(tab)
        button_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Copy button
        ttk.Button(button_frame, text="Copy to Clipboard", command=self.copy_to_clipboard).pack(side=tk.LEFT, padx=5)
        
        # Execute button
        ttk.Button(button_frame, text="Execute Query", command=self.execute_query).pack(side=tk.LEFT, padx=5)
    
    def setup_results_tab(self, tab):
        # Results frame
        results_frame = ttk.LabelFrame(tab, text="Query Results")
        results_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Status frame
        status_frame = ttk.Frame(results_frame)
        status_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Label(status_frame, text="Status:").pack(side=tk.LEFT, padx=5)
        self.status_var = tk.StringVar(value="No query executed yet")
        ttk.Label(status_frame, textvariable=self.status_var).pack(side=tk.LEFT, padx=5)
        
        ttk.Label(status_frame, text="Rows:").pack(side=tk.LEFT, padx=(20, 5))
        self.rows_var = tk.StringVar(value="0")
        ttk.Label(status_frame, textvariable=self.rows_var).pack(side=tk.LEFT, padx=5)
        
        ttk.Label(status_frame, text="Execution time:").pack(side=tk.LEFT, padx=(20, 5))
        self.time_var = tk.StringVar(value="0 ms")
        ttk.Label(status_frame, textvariable=self.time_var).pack(side=tk.LEFT, padx=5)
        
        # Column reordering instructions
        instruction_text = "Drag column headers to reorder columns or right-click for column options"
        ttk.Label(status_frame, text=instruction_text, font=('Arial', 8, 'italic')).pack(side=tk.RIGHT, padx=10)
        
        # Treeview with scrollbars - configure with performance optimizations
        tree_frame = ttk.Frame(results_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Create vertical scrollbar
        vsb = ttk.Scrollbar(tree_frame, orient="vertical")
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Create horizontal scrollbar
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal")
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        
        # Create Treeview with optimized settings
        self.results_tree = ttk.Treeview(
            tree_frame, 
            show="headings",
            selectmode="extended",  # Allow multiple selection
            yscrollcommand=vsb.set,
            xscrollcommand=hsb.set
        )
        self.results_tree.pack(fill=tk.BOTH, expand=True)
        
        # Configure scrollbars
        vsb.configure(command=self.results_tree.yview)
        hsb.configure(command=self.results_tree.xview)
        
        # Alternating row colors
        self.results_tree.tag_configure("oddrow", background="#f9f9f9")
        self.results_tree.tag_configure("evenrow", background="#e6e6e6")
        
        # Variables for tracking column drag operations
        self.drag_start_x = 0
        self.drag_column = ""
        
        # Setup column drag and drop functionality
        self.results_tree.bind("<ButtonPress-1>", self.on_column_drag_start)
        self.results_tree.bind("<B1-Motion>", self.on_column_drag_motion)
        self.results_tree.bind("<ButtonRelease-1>", self.on_column_drag_end)
        
        # Setup context menu for columns
        self.column_menu = tk.Menu(self.results_tree, tearoff=0)
        self.results_tree.bind("<Button-3>", self.show_column_menu)
        
        # Attach a horizontal scroll event to enable column virtualization
        self.results_tree.bind("<Shift-MouseWheel>", self.handle_horizontal_scroll)
        
        # Export frame with additional options
        export_frame = ttk.Frame(tab)
        export_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Button(export_frame, text="Export to CSV", command=self.export_to_csv).pack(side=tk.LEFT, padx=5)
        ttk.Button(export_frame, text="Export to Excel", command=self.export_to_excel).pack(side=tk.LEFT, padx=5)
        
        # Add column optimization buttons
        ttk.Button(
            export_frame, 
            text="Optimize Column Widths", 
            command=self.optimize_column_widths
        ).pack(side=tk.LEFT, padx=20)
        
        # Add a button to reset column order
        ttk.Button(export_frame, text="Reset Column Order", command=self.reset_column_order).pack(side=tk.RIGHT, padx=5)

    def handle_horizontal_scroll(self, event):
        """
        Virtualize columns dynamically based on horizontal scroll.
        Only render/drop columns not currently visible to save resources.
        """
        # Example placeholder for column virtualization logic
        pass

    def optimize_column_widths(self):
        """Optimize column widths based on content"""
        if not hasattr(self, 'result_data') or not self.result_data:
            return
            
        columns = self.results_tree["columns"]
        if not columns:
            return
            
        # Show busy cursor and status message
        self.root.config(cursor="watch")
        self.status_var.set("Optimizing column widths...")
        self.root.update_idletasks()
        
        # Get sample data for width calculation
        visible_items = self.results_tree.get_children()[:20]  # Sample up to 20 visible rows
        
        # Process columns in batches
        batch_size = 30
        for i in range(0, len(columns), batch_size):
            batch_end = min(i + batch_size, len(columns))
            
            for j in range(i, batch_end):
                col = columns[j]
                col_name = self.results_tree.heading(col, "text")
                
                # Start with column name width + padding
                max_width = len(col_name) * 8 + 20
                
                # Check sample values
                for item_id in visible_items:
                    values = self.results_tree.item(item_id, "values")
                    if j < len(values):
                        val_str = str(values[j])
                        # Limit max width to prevent huge columns
                        val_width = min(300, len(val_str) * 7 + 10)
                        max_width = max(max_width, val_width)
                
                # Set reasonable min/max
                width = max(50, min(300, max_width))
                self.results_tree.column(col, width=width)
            
            # Update UI periodically
            if i % batch_size == 0 and i > 0:
                self.root.update_idletasks()
                self.status_var.set(f"Optimizing widths: {i}/{len(columns)} columns")
        
        # Restore cursor and update status
        self.root.config(cursor="")
        self.status_var.set("Column widths optimized")

    def reset_column_order(self):
        """Reset columns to their original order"""
        if not hasattr(self, 'result_data') or not self.result_data:
            return
            
        # Show busy cursor
        self.root.config(cursor="watch")
        self.status_var.set("Resetting column order...")
        self.root.update_idletasks()
        
        # Get original column configuration
        original_columns = list(self.result_data["columns"])
        
        # This approach is more efficient than recreating everything
        # First, we'll create a mapping from current positions to original positions
        current_columns = list(self.results_tree["columns"])
        position_map = {}
        
        for orig_idx, col_name in enumerate(original_columns):
            try:
                curr_idx = current_columns.index(col_name)
                position_map[curr_idx] = orig_idx
            except ValueError:
                # Column might not exist if columns were added/removed
                pass
        
        # Now perform moves to restore original order
        # We'll use a bubble-sort like approach to minimize moves
        for i in range(len(current_columns)):
            for j in range(len(current_columns)-1, i, -1):
                if position_map.get(j-1, j-1) > position_map.get(j, j):
                    # Swap columns j-1 and j
                    self.move_column(j-1, j)
                    # Update our mapping after the swap
                    position_map[j-1], position_map[j] = position_map[j], position_map[j-1]
                    
                    # Allow UI to update
                    if (j % 10 == 0):
                        self.root.update_idletasks()
        
        # Restore original column ordering in result_data if necessary
        self.result_data["columns"] = original_columns
            
        # Restore cursor
        self.root.config(cursor="")
        self.status_var.set("Column order reset to original")

    def show_column_menu(self, event):
        """Enhanced context menu for columns with performance features"""
        region = self.results_tree.identify_region(event.x, event.y)
        if region == "heading":
            # Clear previous menu items
            self.column_menu.delete(0, tk.END)
            
            # Identify which column was clicked
            column = self.results_tree.identify_column(event.x)
            if column:
                column_index = int(column[1:]) - 1
                columns = self.results_tree["columns"]
                
                if 0 <= column_index < len(columns):
                    col_name = self.results_tree.heading(column, "text")
                    
                    # Add column name as menu header (non-clickable)
                    self.column_menu.add_command(label=f"Column: {col_name}", state="disabled")
                    self.column_menu.add_separator()
                    
                    # Add move options
                    self.column_menu.add_command(
                        label="Move First", 
                        command=lambda: self.move_column(column_index, 0))
                    self.column_menu.add_command(
                        label="Move Left", 
                        command=lambda: self.move_column(column_index, max(0, column_index - 1)))
                    self.column_menu.add_command(
                        label="Move Right", 
                        command=lambda: self.move_column(column_index, min(len(columns) - 1, column_index + 1)))
                    self.column_menu.add_command(
                        label="Move Last", 
                        command=lambda: self.move_column(column_index, len(columns) - 1))
                    
                    # Add optimize width option for this column
                    self.column_menu.add_separator()
                    self.column_menu.add_command(
                        label="Optimize This Column Width", 
                        command=lambda: self.optimize_single_column(column_index))
                    
                    # Show the menu
                    self.column_menu.post(event.x_root, event.y_root)

    def optimize_single_column(self, column_index):
        """Optimize width for a single column"""
        columns = self.results_tree["columns"]
        if column_index < 0 or column_index >= len(columns):
            return
            
        col = columns[column_index]
        col_name = self.results_tree.heading(col, "text")
        
        # Start with column name width + padding
        max_width = len(col_name) * 8 + 20
        
        # Sample up to 20 rows for optimization
        visible_items = self.results_tree.get_children()[:20]
        
        # Check sample values
        for item_id in visible_items:
            values = self.results_tree.item(item_id, "values")
            if column_index < len(values):
                val_str = str(values[column_index])
                # Limit max width to prevent huge columns
                val_width = min(300, len(val_str) * 7 + 10)
                max_width = max(max_width, val_width)
        
        # Set reasonable min/max
        width = max(50, min(300, max_width))
        self.results_tree.column(col, width=width)

    def execute_query(self):
        # Get the SQL query from the text area
        sql = self.query_text.get(1.0, tk.END).strip()
        
        if not sql:
            messagebox.showwarning("Empty Query", "Please generate a SQL query first!")
            return
        
        if not self.active_conn or not self.active_cursor:
            messagebox.showwarning("No Connection", "Please connect to a database first!")
            return
        
        try:
            # Reset status
            self.status_var.set("Executing query...")
            self.root.update_idletasks()
            self.rows_var.set("0")
            self.time_var.set("0 ms")
            self.root.update()
            
            # Clear previous results
            for item in self.results_tree.get_children():
                self.results_tree.delete(item)
            
            # Execute query with timing
            start_time = datetime.datetime.now()
            
            self.active_cursor.execute(sql)
            
            # Get column names before fetching data
            column_names = [desc[0] for desc in self.active_cursor.description]
            
            # Check if result set is very large (many columns)
            if len(column_names) > 100:
                if not messagebox.askyesno("Large Result Set", 
                                         f"This query returns {len(column_names)} columns which may cause the application to slow down.\n\n"
                                         "Do you want to continue loading all columns?"):
                    self.status_var.set("Query canceled - too many columns")
                    return
            
            # Configure treeview columns before fetching data - improves performance
            self.setup_result_columns(column_names)
            
            # Fetch and display data in batches
            self.fetch_and_display_data(self.active_cursor)
            
            # Calculate execution time
            end_time = datetime.datetime.now()
            execution_time = (end_time - start_time).total_seconds() * 1000  # Convert to milliseconds
            
            # Update status
            self.status_var.set("Query executed successfully")
            self.rows_var.set(str(len(self.result_data["data"])) if hasattr(self, 'result_data') else "0")
            self.time_var.set(f"{execution_time:.2f} ms")
            
        except Exception as e:
            messagebox.showerror("Query Execution Failed", str(e))
            self.status_var.set(f"Error: {str(e)[:50]}...")

    def setup_result_columns(self, column_names):
        # For very large column sets, display only first 100 columns initially
        if len(column_names) >= 800:
            display_cols = column_names[:100]
            self.visible_columns = display_cols
            self.hidden_columns = column_names[100:]
        else:
            display_cols = column_names
            self.visible_columns = display_cols
            self.hidden_columns = []
        
        # Disable redraw during setup to improve performance
        self.results_tree.configure(show="tree")
        self.results_tree["columns"] = display_cols

        batch_size = 50  # Process columns in batches
        for i in range(0, len(display_cols), batch_size):
            batch_end = min(i + batch_size, len(display_cols))
            for j in range(i, batch_end):
                col = display_cols[j]
                col_width = min(200, max(50, len(col) * 8))
                self.results_tree.heading(col, text=col)
                self.results_tree.column(col, width=col_width)
            if i % batch_size == 0 and i > 0:
                self.root.update_idletasks()
                self.status_var.set(f"Setting up columns ({i}/{len(display_cols)})")
        self.results_tree.configure(show="headings")
        self.root.update_idletasks()

    def fetch_and_display_data(self, cursor):
        """
        Fetch data in smaller batches for large column counts.
        """
        # Fetch in smaller batches to avoid memory issues
        column_count = len(cursor.description)
        if column_count > 300:
            batch_size = 50
        else:
            batch_size = 100
        total_rows = 0
        results = []
        
        # Initialize result data structure to store data for future use
        self.result_data = {
            "columns": [desc[0] for desc in cursor.description],
            "data": []
        }
        
        # Inform user we're fetching data
        self.status_var.set("Fetching data...")
        self.root.update_idletasks()
        
        # Fetch and display in batches
        more_data = True
        while more_data:
            # Fetch next batch
            batch = cursor.fetchmany(batch_size)
            if not batch:
                more_data = False
                continue
                
            # Convert batch to list of lists for easier manipulation
            batch_as_lists = [list(row) for row in batch]
            
            # Append to our stored data
            self.result_data["data"].extend(batch_as_lists)
            
            # Update row count
            total_rows += len(batch)
            self.rows_var.set(str(total_rows))
            
            # Update status every few batches
            if total_rows % 500 == 0:
                self.status_var.set(f"Fetched {total_rows} rows...")
                self.root.update_idletasks()
            
            # Display this batch
            self.display_batch(batch_as_lists, total_rows - len(batch))
        
        # Final status update
        self.status_var.set(f"Fetched {total_rows} rows total")
        self.root.update_idletasks()

    def display_batch(self, batch, start_row):
        """
        Display data lazily, only formatting columns actually visible.
        """
        for row_idx, row in enumerate(batch):
            # Convert any non-serializable types to strings
            formatted_row = []
            for val in row:
                if val is None:
                    formatted_row.append("NULL")
                elif isinstance(val, (datetime.date, datetime.datetime)):
                    formatted_row.append(val.isoformat())
                else:
                    formatted_row.append(str(val))
            # Use alternating row colors
            tag = "evenrow" if (start_row + row_idx) % 2 == 0 else "oddrow"
            self.results_tree.insert("", tk.END, values=formatted_row, tags=(tag,))
        # Allow UI to update occasionally
        if len(batch) > 50:  # Only force updates for larger batches
            self.root.update_idletasks()

    def move_column(self, source_index, target_index):
        """Move a column from source to target position efficiently"""
        if not hasattr(self, 'result_data') or not self.result_data:
            return
            
        # Get column configurations
        columns = list(self.results_tree["columns"])
        if not columns or source_index >= len(columns) or target_index >= len(columns):
            return
            
        # Capture column widths before moving
        column_widths = {}
        for col in columns:
            try:
                column_widths[col] = self.results_tree.column(col, "width")
            except:
                column_widths[col] = 100  # Default width
        
        # Get column headings
        column_texts = {}
        for col in columns:
            try:
                column_texts[col] = self.results_tree.heading(col, "text")
            except:
                column_texts[col] = col  # Default to column ID
        
        # Move column in the list
        column_to_move = columns.pop(source_index)
        columns.insert(target_index, column_to_move)
        
        # Update the result data structure column list
        col_name = self.result_data["columns"].pop(source_index)
        self.result_data["columns"].insert(target_index, col_name)
        
        # Disable UI updates during reconfiguration
        self.root.config(cursor="watch")  # Show busy cursor
        self.status_var.set("Rearranging columns...")
        self.root.update_idletasks()
        
        # We'll reuse the current treeview rather than recreating it
        
        # First, save all data and clear treeview
        items_data = []
        for item in self.results_tree.get_children():
            values = list(self.results_tree.item(item, "values"))
            # Move the value within the row
            if source_index < len(values) and target_index < len(values):
                val = values.pop(source_index)
                values.insert(target_index, val)
            items_data.append((values, self.results_tree.item(item, "tags")))
        
        # Clear current rows
        self.results_tree.delete(*self.results_tree.get_children())
        
        # Update the underlying data to match new column order
        for i, row in enumerate(self.result_data["data"]):
            if source_index < len(row) and target_index < len(row):
                val = row.pop(source_index)
                row.insert(target_index, val)
        
        # Temporarily hide treeview headers to reduce flicker
        self.results_tree.configure(show="tree")
        
        # Update column configuration
        self.results_tree["columns"] = columns
        
        # Restore column headings and widths
        for i, col in enumerate(columns):
            self.results_tree.heading(col, text=column_texts.get(col, col))
            self.results_tree.column(col, width=column_widths.get(col, 100))
        
        # Show headings again
        self.results_tree.configure(show="headings")
        
        # Process items in batches to prevent UI freezing
        batch_size = 100
        for i in range(0, len(items_data), batch_size):
            batch_end = min(i + batch_size, len(items_data))
            for j in range(i, batch_end):
                values, tags = items_data[j]
                self.results_tree.insert("", tk.END, values=values, tags=tags)
            
            # Update UI occasionally
            if i > 0 and i % 500 == 0:
                self.root.update_idletasks()
                self.status_var.set(f"Reloading rows: {i}/{len(items_data)}")
        
        # Restore cursor and status
        self.root.config(cursor="")
        self.status_var.set("Column rearranged successfully")

    def on_column_drag_start(self, event):
        """Start column drag operation"""
        # Identify the column based on mouse position
        region = self.results_tree.identify_region(event.x, event.y)
        if region == "heading":
            column = self.results_tree.identify_column(event.x)
            # Convert column identifier (e.g. #1, #2) to column name
            if column:
                column_index = int(column[1:]) - 1
                if 0 <= column_index < len(self.results_tree["columns"]):
                    # Store the starting position and column being dragged
                    self.drag_start_x = event.x
                    self.drag_column = column
                    # Set cursor to indicate dragging
                    self.results_tree.config(cursor="fleur")

    def on_column_drag_motion(self, event):
        """Handle column drag motion"""
        if self.drag_column:
            # Change cursor while dragging
            self.results_tree.config(cursor="fleur")

    def on_column_drag_end(self, event):
        """Complete column drag operation"""
        if self.drag_column:
            # Identify target column
            target_column = self.results_tree.identify_column(event.x)
            if target_column and target_column != self.drag_column:
                # Get column names and indices
                columns = self.results_tree["columns"]
                source_index = int(self.drag_column[1:]) - 1
                target_index = int(target_column[1:]) - 1
                
                if 0 <= source_index < len(columns) and 0 <= target_index < len(columns):
                    # Reorder columns
                    self.move_column(source_index, target_index)
            
            # Reset drag state
            self.drag_column = ""
            self.drag_start_x = 0
            self.results_tree.config(cursor="")

    def export_to_csv(self):
        """Export query results to CSV file"""
        if not hasattr(self, 'result_data') or not self.result_data:
            messagebox.showwarning("No Data", "There is no data to export!")
            return
        
        try:
            file_path = filedialog.asksaveasfilename(
                defaultextension=".csv",
                filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]
            )
            
            if not file_path:
                return  # User canceled
            
            # Show busy cursor
            self.root.config(cursor="watch")
            self.status_var.set("Exporting to CSV...")
            self.root.update_idletasks()
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                
                # Write header
                writer.writerow(self.result_data["columns"])
                
                # Write data in batches
                batch_size = 1000
                for i in range(0, len(self.result_data["data"]), batch_size):
                    batch_end = min(i + batch_size, len(self.result_data["data"]))
                    for j in range(i, batch_end):
                        row = self.result_data["data"][j]
                        # Convert any non-serializable types to strings
                        formatted_row = []
                        for val in row:
                            if val is None:
                                formatted_row.append("")
                            elif isinstance(val, (datetime.date, datetime.datetime)):
                                formatted_row.append(val.isoformat())
                            else:
                                formatted_row.append(val)
                        
                        writer.writerow(formatted_row)
                    
                    # Update status occasionally for large datasets
                    if i % 5000 == 0 and i > 0:
                        self.status_var.set(f"Exporting to CSV: {i}/{len(self.result_data['data'])} rows...")
                        self.root.update_idletasks()
            
            # Restore cursor and show success message
            self.root.config(cursor="")
            self.status_var.set("Export complete")
            messagebox.showinfo("Export Success", f"Data exported to {file_path}")
            
        except Exception as e:
            self.root.config(cursor="")
            messagebox.showerror("Export Failed", str(e))
    
    def export_to_excel(self):
        """Export query results to Excel file"""
        if not hasattr(self, 'result_data') or not self.result_data:
            messagebox.showwarning("No Data", "There is no data to export!")
            return
        
        try:
            # Check if pandas is available
            if 'pandas' not in sys.modules:
                messagebox.showinfo(
                    "Module Required", 
                    "Exporting to Excel requires the pandas and openpyxl modules.\n"
                    "Please install them with: pip install pandas openpyxl"
                )
                return
            
            file_path = filedialog.asksaveasfilename(
                defaultextension=".xlsx",
                filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
            )
            
            if not file_path:
                return  # User canceled
            
            # Show busy cursor
            self.root.config(cursor="watch")
            self.status_var.set("Exporting to Excel...")
            self.root.update_idletasks()
            
            # Convert data to DataFrame
            # For large datasets, create DataFrame more efficiently
            if len(self.result_data["data"]) > 10000:
                # Process in chunks to avoid memory issues
                chunk_size = 5000
                dfs = []
                
                for i in range(0, len(self.result_data["data"]), chunk_size):
                    end_idx = min(i + chunk_size, len(self.result_data["data"]))
                    chunk = self.result_data["data"][i:end_idx]
                    
                    # Convert any non-serializable types
                    processed_chunk = []
                    for row in chunk:
                        processed_row = []
                        for val in row:
                            if isinstance(val, (datetime.date, datetime.datetime)):
                                processed_row.append(val.isoformat())
                            else:
                                processed_row.append(val)
                        processed_chunk.append(processed_row)
                    
                    dfs.append(pd.DataFrame(processed_chunk, columns=self.result_data["columns"]))
                    
                    # Update status occasionally
                    self.status_var.set(f"Processing data: {end_idx}/{len(self.result_data['data'])} rows...")
                    self.root.update_idletasks()
                
                # Combine all chunks
                df = pd.concat(dfs, ignore_index=True)
            else:
                # For smaller datasets, create DataFrame directly
                df = pd.DataFrame(self.result_data["data"], columns=self.result_data["columns"])
            
            # Export to Excel
            self.status_var.set("Writing to Excel file...")
            self.root.update_idletasks()
            
            df.to_excel(file_path, index=False)
            
            # Restore cursor and show success message
            self.root.config(cursor="")
            self.status_var.set("Export complete")
            messagebox.showinfo("Export Success", f"Data exported to {file_path}")
            
        except Exception as e:
            self.root.config(cursor="")
            messagebox.showerror("Export Failed", str(e))

    def __del__(self):
        # Cleanup database connections when app closes
        try:
            if hasattr(self, 'active_cursor') and self.active_cursor:
                self.active_cursor.close()
            if hasattr(self, 'active_conn') and self.active_conn:
                self.active_conn.close()
        except:
            pass

    # (Optional) Simple tooltip function:
    def create_tooltip(self, widget, text):
        tip = tk.Toplevel(widget, bg="#ffffe0", padx=5, pady=5)
        tip.withdraw()
        tip.overrideredirect(True)
        ttk.Label(tip, text=text).pack()
        def enter(event):
            tip.deiconify()
            x, y = event.x_root + 10, event.y_root + 10
            tip.geometry(f"+{x}+{y}")
        def leave(event):
            tip.withdraw()
        widget.bind("<Enter>", enter)
        widget.bind("<Leave>", leave)

if __name__ == "__main__":
    root = tk.Tk()
    app = SQLDataFetcher(root)
    root.mainloop()
