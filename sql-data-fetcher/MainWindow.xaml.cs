using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Data;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Threading;
using Microsoft.Data.SqlClient;
using Microsoft.Win32;
using MySql.Data.MySqlClient;
using SQLDataFetcher.Models;
using SQLDataFetcher.Services;
using CsvHelper;
using System.Globalization;
using OfficeOpenXml;
using System.Windows.Documents;
using System.Security.Cryptography;

namespace SQLDataFetcher
{
    public partial class MainWindow : Window
    {
        // Constants
        private readonly string[] AGGREGATE_FUNCTIONS = { "COUNT", "SUM", "AVG", "MIN", "MAX" };
        private readonly string[] SORT_ORDERS = { "ASC", "DESC" };
        private readonly string[] JOIN_TYPES = { "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN", "CROSS JOIN" };

        // Database connection
        private IDbConnection? activeConn;
        private IDbCommand? activeCommand;

        // Tables and columns data
        private Dictionary<string, List<string>> tables = new();
        private List<string> selectedTables = new();
        private Dictionary<string, Dictionary<string, bool>> selectedColumns = new();

        // SQL operations 
        private List<OrderByEntry> orderByEntries = new();
        private List<GroupByEntry> groupByEntries = new();
        private List<AggregateEntry> aggregateEntries = new();
        private List<WhereEntry> whereEntries = new();
        private List<CombinedColumnEntry> combinedColumnEntries = new();
        private List<JoinEntry> joinEntries = new();

        // Results data
        private DataTable? resultData;
        private List<string> originalColumnOrder = new();

        // AI Assistant variables
        private GeminiService? geminiService;
        private string apiKeyFileName = "api_key.enc";

        public MainWindow()
        {
            InitializeComponent();
            
            // Replace the direct Loaded event with a proper event subscription
            // to ensure we don't try to access controls before they're initialized

            // Set license context for EPPlus
            ExcelPackage.LicenseContext = LicenseContext.NonCommercial;

            // Initialize AI Assistant
            InitializeAiAssistant();
        }

        private void UpdateUIForDatabaseType()
        {
            // Use additional defensive coding with more specific null checks
            if (DbTypeComboBox?.SelectedItem == null) return;
            if (PortTextBox == null || AuthTypeComboBox == null || 
                UsernameTextBox == null || PasswordBox == null) 
            {
                return;
            }

            // Get the content from the selected item safely
            var selectedItem = DbTypeComboBox.SelectedItem as ComboBoxItem;
            if (selectedItem == null) return;

            var isMySQL = selectedItem.Content?.ToString() == "MySQL";
            
            PortTextBox.IsEnabled = isMySQL;

            if (isMySQL)
            {
                // MySQL always uses username/password
                AuthTypeComboBox.SelectedIndex = 1; // Select SQL Server Authentication
                UsernameTextBox.IsEnabled = true;
                PasswordBox.IsEnabled = true;
            }
            else
            {
                UpdateUIForAuthType();
            }
        }

        private void UpdateUIForAuthType()
        {
            // Guard against null controls during initialization
            if (AuthTypeComboBox == null || DbTypeComboBox == null || 
                UsernameTextBox == null || PasswordBox == null)
            {
                return;
            }

            var isWindowsAuth = ((ComboBoxItem)AuthTypeComboBox.SelectedItem).Content.ToString() == "Windows Authentication";
            var isMySQL = ((ComboBoxItem)DbTypeComboBox.SelectedItem).Content.ToString() == "MySQL";

            // MySQL always uses username/password auth
            if (isMySQL)
            {
                UsernameTextBox.IsEnabled = true;
                PasswordBox.IsEnabled = true;
            }
            else
            {
                // SQL Server can use Windows auth
                UsernameTextBox.IsEnabled = !isWindowsAuth;
                PasswordBox.IsEnabled = !isWindowsAuth;
            }
        }

        private void DbTypeComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            UpdateUIForDatabaseType();
        }

        private void AuthTypeComboBox_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            UpdateUIForAuthType();
        }

        private void ConnectButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var dbType = ((ComboBoxItem)DbTypeComboBox.SelectedItem).Content.ToString();
                var authType = ((ComboBoxItem)DbTypeComboBox.SelectedItem).Content.ToString();
                var server = ServerTextBox.Text;
                var database = DatabaseTextBox.Text;
                var username = UsernameTextBox.Text;
                var password = PasswordBox.Password;
                int port = 3306; // Default MySQL port

                if (string.IsNullOrEmpty(server) || string.IsNullOrEmpty(database))
                {
                    MessageBox.Show("Server and database name are required.", "Connection Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    return;
                }

                if (dbType == "MySQL" && !int.TryParse(PortTextBox.Text, out port))
                {
                    MessageBox.Show("Port must be a valid number.", "Connection Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    return;
                }

                // Close any existing connection
                CloseConnection();

                if (dbType == "MySQL")
                {
                    ConnectToMySql(server, port, database, username, password);
                }
                else
                {
                    ConnectToSqlServer(server, database, authType == "Windows Authentication", username, password);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Connection failed: {ex.Message}", "Connection Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void ConnectToSqlServer(string server, string database, bool useWindowsAuth, string username, string password)
        {
            // Build connection string
            var connStrBuilder = new SqlConnectionStringBuilder
            {
                DataSource = server,
                InitialCatalog = database,
                IntegratedSecurity = useWindowsAuth,
                TrustServerCertificate = true
            };

            if (!useWindowsAuth)
            {
                connStrBuilder.UserID = username;
                connStrBuilder.Password = password;
            }

            try
            {
                var conn = new SqlConnection(connStrBuilder.ConnectionString);
                conn.Open();

                activeConn = conn;
                activeCommand = conn.CreateCommand();

                LoadTablesFromSqlServer(conn, database);

                MessageBox.Show($"Connected to SQL Server database {database} successfully.\nFound {tables.Count} tables.", 
                    "Success", MessageBoxButton.OK, MessageBoxImage.Information);

                // Move to the next tab
                MainTabControl.SelectedIndex = 1;
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to connect to SQL Server: {ex.Message}");
            }
        }

        private void ConnectToMySql(string server, int port, string database, string username, string password)
        {
            try
            {
                // Build connection string
                var connStrBuilder = new MySqlConnectionStringBuilder
                {
                    Server = server,
                    Port = Convert.ToUInt32(port),
                    Database = database,
                    UserID = username,
                    Password = password
                };

                var conn = new MySqlConnection(connStrBuilder.ConnectionString);
                conn.Open();

                activeConn = conn;
                activeCommand = conn.CreateCommand();

                LoadTablesFromMySql(conn);

                MessageBox.Show($"Connected to MySQL database {database} successfully.\nFound {tables.Count} tables.", 
                    "Success", MessageBoxButton.OK, MessageBoxImage.Information);

                // Move to the next tab
                MainTabControl.SelectedIndex = 1;
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to connect to MySQL: {ex.Message}");
            }
        }

        private void LoadTablesFromSqlServer(SqlConnection conn, string database)
        {
            tables.Clear();
            TablesListView.Items.Clear();

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_CATALOG = @Database";
                cmd.Parameters.AddWithValue("@Database", database);
                
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var tableName = reader.GetString(0);
                    if (!string.IsNullOrEmpty(tableName)) // Null check
                    {
                        TablesListView.Items.Add(tableName);
                    }
                }
            }

            // Get column info for each table
            foreach (var tableName in TablesListView.Items.Cast<string>().ToList())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @TableName";
                    cmd.Parameters.AddWithValue("@TableName", tableName);

                    var columns = new List<string>();
                    
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string columnName = reader.GetString(0);
                            if (!string.IsNullOrEmpty(columnName)) // Null check
                            {
                                columns.Add(columnName);
                            }
                        }
                    }

                    tables[tableName] = columns;
                }
            }
        }

        private void LoadTablesFromMySql(MySqlConnection conn)
        {
            tables.Clear();
            TablesListView.Items.Clear();

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "SHOW TABLES";
                
                using var reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var tableName = reader.GetString(0);
                    if (!string.IsNullOrEmpty(tableName)) // Null check
                    {
                        TablesListView.Items.Add(tableName);
                    }
                }
            }

            // Get column info for each table
            foreach (var tableName in TablesListView.Items.Cast<string>().ToList())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"SHOW COLUMNS FROM `{tableName}`";

                    var columns = new List<string>();
                    
                    using (var reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            string columnName = reader.GetString(0);
                            if (!string.IsNullOrEmpty(columnName)) // Null check
                            {
                                columns.Add(columnName);
                            }
                        }
                    }

                    tables[tableName] = columns;
                }
            }
        }

        private void CloseConnection()
        {
            activeCommand?.Dispose();
            activeCommand = null;

            if (activeConn != null)
            {
                activeConn.Close();
                activeConn.Dispose();
                activeConn = null;
            }
        }

        private void ContinueToColumnsButton_Click(object sender, RoutedEventArgs e)
        {
            if (TablesListView.SelectedItems.Count < 1)
            {
                MessageBox.Show("Please select at least one table.", "Table Selection", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            selectedTables = TablesListView.SelectedItems.Cast<string>().ToList();
            
            // Clear any existing join entries when starting a new query
            joinEntries.Clear();
            
            // If there's only one table, skip the join configuration tab
            if (selectedTables.Count == 1)
            {
                SetupColumnsTab();
                MainTabControl.SelectedIndex = 3; // Skip to Select Columns tab
            }
            else
            {
                // If there are multiple tables, go to the join configuration tab
                MainTabControl.SelectedIndex = 2;
            }
        }

        private void AddJoinConfiguration_Click(object sender, RoutedEventArgs e)
        {
            if (selectedTables.Count < 2)
            {
                MessageBox.Show("You need at least two tables to configure joins.", 
                    "Join Configuration", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
            
            // Create a new join entry
            var entry = new JoinEntry();
            joinEntries.Add(entry);
            
            // Create a GroupBox for this join configuration
            var groupBox = new GroupBox
            {
                Header = $"Join Configuration #{joinEntries.Count}",
                Margin = new Thickness(0, 0, 0, 20),
                Padding = new Thickness(10)
            };
            
            // Create a Grid for the join configuration controls
            var grid = new Grid();
            groupBox.Content = grid;
            
            // Define the grid columns and rows
            grid.ColumnDefinitions.Add(new ColumnDefinition { Width = GridLength.Auto });
            grid.ColumnDefinitions.Add(new ColumnDefinition { Width = new GridLength(1, GridUnitType.Star) });
            
            int rowCount = 0;
            
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            
            // First join has different layout than subsequent joins
            bool isFirstJoin = joinEntries.Count == 1;
            
            // Left Table (only for the first join, as subsequent joins will use the right table of the previous join)
            if (isFirstJoin)
            {
                var leftTableLabel = new Label { Content = "Left Table:", VerticalAlignment = VerticalAlignment.Center };
                Grid.SetRow(leftTableLabel, rowCount);
                Grid.SetColumn(leftTableLabel, 0);
                grid.Children.Add(leftTableLabel);
                
                var leftTableCombo = new ComboBox
                {
                    MinWidth = 200,
                    Margin = new Thickness(5),
                    IsEditable = false
                };
                
                // Populate with tables
                foreach (var tableName in selectedTables)
                {
                    leftTableCombo.Items.Add(tableName);
                }
                
                // Select the first table by default
                if (leftTableCombo.Items.Count > 0)
                {
                    leftTableCombo.SelectedIndex = 0;
                }
                
                Grid.SetRow(leftTableCombo, rowCount);
                Grid.SetColumn(leftTableCombo, 1);
                grid.Children.Add(leftTableCombo);
                
                // Store reference to the ComboBox
                entry.LeftTableComboBox = leftTableCombo;
                
                // Add event handler to update left column options when left table changes
                leftTableCombo.SelectionChanged += (s, args) => UpdateJoinColumnOptions(entry);
                
                rowCount++;
                grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            }
            else
            {
                // For subsequent joins, the left table is the right table of the previous join
                var previousJoin = joinEntries[joinEntries.Count - 2];
                string previousRightTable = previousJoin.RightTableComboBox?.SelectedItem?.ToString() ?? "";
                
                // Create a hidden ComboBox to store the left table value (needed for internal logic)
                var leftTableCombo = new ComboBox
                {
                    Visibility = Visibility.Collapsed
                };
                leftTableCombo.Items.Add(previousRightTable);
                leftTableCombo.SelectedIndex = 0;
                
                grid.Children.Add(leftTableCombo);
                
                // Store reference to the ComboBox
                entry.LeftTableComboBox = leftTableCombo;
            }
            
            // Join Type
            var joinTypeLabel = new Label { Content = "Join Type:", VerticalAlignment = VerticalAlignment.Center };
            Grid.SetRow(joinTypeLabel, rowCount);
            Grid.SetColumn(joinTypeLabel, 0);
            grid.Children.Add(joinTypeLabel);
            
            var joinTypeCombo = new ComboBox
            {
                MinWidth = 200,
                Margin = new Thickness(5)
            };
            
            // Populate with join types
            foreach (var joinType in JOIN_TYPES)
            {
                joinTypeCombo.Items.Add(joinType);
            }
            
            // Default to INNER JOIN
            joinTypeCombo.SelectedIndex = 0;
            
            Grid.SetRow(joinTypeCombo, rowCount);
            Grid.SetColumn(joinTypeCombo, 1);
            grid.Children.Add(joinTypeCombo);
            
            // Store reference to the ComboBox
            entry.JoinTypeComboBox = joinTypeCombo;
            
            rowCount++;
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            
            // Right Table
            var rightTableLabel = new Label { Content = "Right Table:", VerticalAlignment = VerticalAlignment.Center };
            Grid.SetRow(rightTableLabel, rowCount);
            Grid.SetColumn(rightTableLabel, 0);
            grid.Children.Add(rightTableLabel);
            
            var rightTableCombo = new ComboBox
            {
                MinWidth = 200,
                Margin = new Thickness(5),
                IsEditable = false
            };
            
            // Populate with tables, excluding the left table if it's the first join
            if (isFirstJoin)
            {
                string leftTable = entry.LeftTableComboBox?.SelectedItem?.ToString() ?? "";
                
                foreach (var tableName in selectedTables)
                {
                    if (tableName != leftTable)
                    {
                        rightTableCombo.Items.Add(tableName);
                    }
                }
            }
            else
            {
                // For subsequent joins, exclude tables already used in previous joins
                var usedTables = new HashSet<string>();
                foreach (var join in joinEntries)
                {
                    if (join.LeftTableComboBox?.SelectedItem != null)
                        usedTables.Add(join.LeftTableComboBox.SelectedItem.ToString() ?? "");
                    
                    if (join == entry) // Stop at the current join
                        break;
                }
                
                foreach (var tableName in selectedTables)
                {
                    if (!usedTables.Contains(tableName))
                    {
                        rightTableCombo.Items.Add(tableName);
                    }
                }
            }
            
            // Select the first available table by default
            if (rightTableCombo.Items.Count > 0)
            {
                rightTableCombo.SelectedIndex = 0;
            }
            
            Grid.SetRow(rightTableCombo, rowCount);
            Grid.SetColumn(rightTableCombo, 1);
            grid.Children.Add(rightTableCombo);
            
            // Store reference to the ComboBox
            entry.RightTableComboBox = rightTableCombo;
            
            // Add event handler to update right column options when right table changes
            rightTableCombo.SelectionChanged += (s, args) => UpdateJoinColumnOptions(entry);
            
            // Add event handler to update available right tables when left table changes (first join only)
            if (isFirstJoin && entry.LeftTableComboBox != null)
            {
                entry.LeftTableComboBox.SelectionChanged += (s, args) =>
                {
                    var leftTable = entry.LeftTableComboBox?.SelectedItem?.ToString();
                    rightTableCombo.Items.Clear();
                    
                    foreach (var tableName in selectedTables)
                    {
                        if (tableName != leftTable)
                        {
                            rightTableCombo.Items.Add(tableName);
                        }
                    }
                    
                    if (rightTableCombo.Items.Count > 0)
                    {
                        rightTableCombo.SelectedIndex = 0;
                    }
                    
                    UpdateJoinColumnOptions(entry);
                };
            }
            
            rowCount++;
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            
            // Join ON condition
            var onLabel = new Label { Content = "ON:", VerticalAlignment = VerticalAlignment.Center };
            Grid.SetRow(onLabel, rowCount);
            Grid.SetColumn(onLabel, 0);
            grid.Children.Add(onLabel);
            
            // ON condition panel
            var onPanel = new StackPanel { Orientation = Orientation.Horizontal };
            Grid.SetRow(onPanel, rowCount);
            Grid.SetColumn(onPanel, 1);
            grid.Children.Add(onPanel);
            
            // Left column
            var leftColumnCombo = new ComboBox
            {
                MinWidth = 150,
                Margin = new Thickness(5),
                IsEditable = true,
                StaysOpenOnEdit = true
            };
            
            onPanel.Children.Add(leftColumnCombo);
            
            // Equals label
            onPanel.Children.Add(new Label { Content = " = ", VerticalAlignment = VerticalAlignment.Center });
            
            // Right column
            var rightColumnCombo = new ComboBox
            {
                MinWidth = 150,
                Margin = new Thickness(5),
                IsEditable = true,
                StaysOpenOnEdit = true
            };
            
            onPanel.Children.Add(rightColumnCombo);
            
            // Store references to the ComboBoxes
            entry.LeftColumnComboBox = leftColumnCombo;
            entry.RightColumnComboBox = rightColumnCombo;
            
            rowCount++;
            grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            
            // Remove button
            var removeButton = new Button
            {
                Content = "Remove Join",
                Margin = new Thickness(5),
                HorizontalAlignment = HorizontalAlignment.Right
            };
            
            removeButton.Click += (s, args) =>
            {
                // Remove the entry from the list
                joinEntries.Remove(entry);
                
                // Remove the GroupBox from the panel
                JoinConfigurationPanel.Children.Remove(groupBox);
                
                // Renumber the remaining joins
                int joinNumber = 1;
                foreach (var child in JoinConfigurationPanel.Children)
                {
                    if (child is GroupBox box && box.Header.ToString().StartsWith("Join Configuration #"))
                    {
                        box.Header = $"Join Configuration #{joinNumber}";
                        joinNumber++;
                    }
                }
            };
            
            Grid.SetRow(removeButton, rowCount);
            Grid.SetColumn(removeButton, 1);
            grid.Children.Add(removeButton);
            
            // Add the groupbox to the panel right before the "Add Join Configuration" button
            JoinConfigurationPanel.Children.Insert(1, groupBox);
            
            // Initialize column options based on selected tables
            UpdateJoinColumnOptions(entry);
        }

        private void UpdateJoinColumnOptions(JoinEntry entry)
        {
            if (entry.LeftTableComboBox == null || entry.RightTableComboBox == null ||
                entry.LeftColumnComboBox == null || entry.RightColumnComboBox == null)
                return;
            
            string leftTable = entry.LeftTableComboBox.SelectedItem?.ToString() ?? "";
            string rightTable = entry.RightTableComboBox.SelectedItem?.ToString() ?? "";
            
            // Clear current options
            entry.LeftColumnComboBox.Items.Clear();
            entry.RightColumnComboBox.Items.Clear();
            
            // Add column options for left table
            if (!string.IsNullOrEmpty(leftTable) && tables.ContainsKey(leftTable))
            {
                foreach (var column in tables[leftTable])
                {
                    entry.LeftColumnComboBox.Items.Add($"{leftTable}.{column}");
                }
            }
            
            // Add column options for right table
            if (!string.IsNullOrEmpty(rightTable) && tables.ContainsKey(rightTable))
            {
                foreach (var column in tables[rightTable])
                {
                    entry.RightColumnComboBox.Items.Add($"{rightTable}.{column}");
                }
            }
            
            // Select first options by default if available
            if (entry.LeftColumnComboBox.Items.Count > 0)
            {
                entry.LeftColumnComboBox.SelectedIndex = 0;
            }
            
            if (entry.RightColumnComboBox.Items.Count > 0)
            {
                entry.RightColumnComboBox.SelectedIndex = 0;
            }
            
            // Try to find matching column names for smart defaults
            TryMatchJoinColumns(entry);
        }
        
        private void TryMatchJoinColumns(JoinEntry entry)
        {
            if (entry.LeftTableComboBox == null || entry.RightTableComboBox == null ||
                entry.LeftColumnComboBox == null || entry.RightColumnComboBox == null)
                return;
                
            string leftTable = entry.LeftTableComboBox.SelectedItem?.ToString() ?? "";
            string rightTable = entry.RightTableComboBox.SelectedItem?.ToString() ?? "";
            
            if (string.IsNullOrEmpty(leftTable) || string.IsNullOrEmpty(rightTable))
                return;
                
            if (!tables.ContainsKey(leftTable) || !tables.ContainsKey(rightTable))
                return;
                
            // Look for common column names that might be join keys
            var commonKeys = new List<string> { "id", "ID", "Id", "_id", "key" };
            
            // Append table name to common keys to look for foreign keys
            commonKeys.Add(rightTable + "_id");
            commonKeys.Add(rightTable + "Id");
            commonKeys.Add(rightTable + "ID");
            commonKeys.Add(leftTable + "_id");
            commonKeys.Add(leftTable + "Id");
            commonKeys.Add(leftTable + "ID");
            
            // Look for exact matches first
            foreach (var leftCol in tables[leftTable])
            {
                foreach (var rightCol in tables[rightTable])
                {
                    if (leftCol.Equals(rightCol, StringComparison.OrdinalIgnoreCase))
                    {
                        // Found a matching column name
                        entry.LeftColumnComboBox.SelectedItem = $"{leftTable}.{leftCol}";
                        entry.RightColumnComboBox.SelectedItem = $"{rightTable}.{rightCol}";
                        return;
                    }
                }
            }
            
            // Next, look for common key patterns
            foreach (var key in commonKeys)
            {
                // Check if both tables have this column
                var leftMatch = tables[leftTable].FirstOrDefault(c => c.Equals(key, StringComparison.OrdinalIgnoreCase));
                var rightMatch = tables[rightTable].FirstOrDefault(c => c.Equals(key, StringComparison.OrdinalIgnoreCase));
                
                if (!string.IsNullOrEmpty(leftMatch) && !string.IsNullOrEmpty(rightMatch))
                {
                    entry.LeftColumnComboBox.SelectedItem = $"{leftTable}.{leftMatch}";
                    entry.RightColumnComboBox.SelectedItem = $"{rightTable}.{rightMatch}";
                    return;
                }
            }
            
            // Look for foreign key patterns (e.g., table_id in one table matching id in another)
            var leftId = tables[leftTable].FirstOrDefault(c => c.Equals("id", StringComparison.OrdinalIgnoreCase));
            var rightTableId = tables[rightTable].FirstOrDefault(c => 
                c.Equals(leftTable + "_id", StringComparison.OrdinalIgnoreCase) || 
                c.Equals(leftTable + "Id", StringComparison.OrdinalIgnoreCase) ||
                c.Equals(leftTable + "ID", StringComparison.OrdinalIgnoreCase));
                
            if (!string.IsNullOrEmpty(leftId) && !string.IsNullOrEmpty(rightTableId))
            {
                entry.LeftColumnComboBox.SelectedItem = $"{leftTable}.{leftId}";
                entry.RightColumnComboBox.SelectedItem = $"{rightTable}.{rightTableId}";
                return;
            }
            
            // Check the opposite foreign key pattern
            var rightId = tables[rightTable].FirstOrDefault(c => c.Equals("id", StringComparison.OrdinalIgnoreCase));
            var leftTableId = tables[leftTable].FirstOrDefault(c => 
                c.Equals(rightTable + "_id", StringComparison.OrdinalIgnoreCase) || 
                c.Equals(rightTable + "Id", StringComparison.OrdinalIgnoreCase) ||
                c.Equals(rightTable + "ID", StringComparison.OrdinalIgnoreCase));
                
            if (!string.IsNullOrEmpty(rightId) && !string.IsNullOrEmpty(leftTableId))
            {
                entry.LeftColumnComboBox.SelectedItem = $"{leftTable}.{leftTableId}";
                entry.RightColumnComboBox.SelectedItem = $"{rightTable}.{rightId}";
                return;
            }
        }

        private void SetupColumnsTab()
        {
            // Clear existing tabs and selected columns dictionary
            ColumnsTabControl.Items.Clear();
            selectedColumns.Clear();
            
            // Create a tab for each selected table
            foreach (var tableName in selectedTables)
            {
                // Create a new TabItem
                var tabItem = new TabItem
                {
                    Header = tableName
                };
                
                // Create a grid for the tab content
                var grid = new Grid();
                grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto }); // Search box
                grid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto }); // Button panel
                grid.RowDefinitions.Add(new RowDefinition { Height = new GridLength(1, GridUnitType.Star) }); // Columns list
                
                // Create search panel
                var searchPanel = new DockPanel { Margin = new Thickness(5) };
                var searchLabel = new Label { Content = "Search:", VerticalAlignment = VerticalAlignment.Center };
                var searchBox = new TextBox 
                { 
                    Margin = new Thickness(5),
                    Width = 300,
                    HorizontalAlignment = HorizontalAlignment.Left,
                    VerticalAlignment = VerticalAlignment.Center
                };
                
                // Add clear button
                var clearButton = new Button
                {
                    Content = "✕",
                    Width = 20,
                    Height = 20,
                    Padding = new Thickness(0),
                    Margin = new Thickness(5, 0, 0, 0),
                    VerticalAlignment = VerticalAlignment.Center,
                    ToolTip = "Clear search"
                };
                clearButton.Click += (s, e) => searchBox.Clear();
                
                searchPanel.Children.Add(searchLabel);
                searchPanel.Children.Add(searchBox);
                searchPanel.Children.Add(clearButton);
                
                Grid.SetRow(searchPanel, 0);
                grid.Children.Add(searchPanel);
                
                // Create button panel for Select All/Deselect All
                var buttonPanel = new StackPanel
                {
                    Orientation = Orientation.Horizontal,
                    Margin = new Thickness(5)
                };
                
                var selectAllButton = new Button
                {
                    Content = "Select All",
                    Margin = new Thickness(5),
                    Padding = new Thickness(5, 2, 5, 2)
                };
                selectAllButton.Click += (s, e) => SelectAllColumns(tableName, true);
                
                var deselectAllButton = new Button
                {
                    Content = "Deselect All",
                    Margin = new Thickness(5),
                    Padding = new Thickness(5, 2, 5, 2)
                };
                deselectAllButton.Click += (s, e) => SelectAllColumns(tableName, false);
                
                var selectVisibleButton = new Button
                {
                    Content = "Select Visible",
                    Margin = new Thickness(5),
                    Padding = new Thickness(5, 2, 5, 2),
                    ToolTip = "Select only columns visible in search results"
                };
                selectVisibleButton.Click += (s, e) => SelectVisibleColumns(tableName, true);
                
                var deselectVisibleButton = new Button
                {
                    Content = "Deselect Visible",
                    Margin = new Thickness(5),
                    Padding = new Thickness(5, 2, 5, 2),
                    ToolTip = "Deselect only columns visible in search results"
                };
                deselectVisibleButton.Click += (s, e) => SelectVisibleColumns(tableName, false);
                
                buttonPanel.Children.Add(selectAllButton);
                buttonPanel.Children.Add(deselectAllButton);
                buttonPanel.Children.Add(selectVisibleButton);
                buttonPanel.Children.Add(deselectVisibleButton);
                
                // Add button panel to grid
                Grid.SetRow(buttonPanel, 1);
                grid.Children.Add(buttonPanel);
                
                // Create a ScrollViewer for the checkboxes
                var scrollViewer = new ScrollViewer
                {
                    VerticalScrollBarVisibility = ScrollBarVisibility.Auto
                };
                
                // Create a StackPanel for checkboxes
                var columnsPanel = new StackPanel
                {
                    Margin = new Thickness(5)
                };
                
                // Initialize the selected columns dictionary for this table
                selectedColumns[tableName] = new Dictionary<string, bool>();
                
                // Add a checkbox for each column
                if (tables.ContainsKey(tableName))
                {
                    foreach (var columnName in tables[tableName])
                    {
                        var checkbox = new CheckBox
                        {
                            Content = columnName,
                            IsChecked = true,
                            Margin = new Thickness(5)
                        };
                        
                        // Track selection state
                        selectedColumns[tableName][columnName] = true;
                        
                        // Update the selection state when toggled
                        checkbox.Checked += (s, e) => selectedColumns[tableName][columnName] = true;
                        checkbox.Unchecked += (s, e) => selectedColumns[tableName][columnName] = false;
                        
                        columnsPanel.Children.Add(checkbox);
                    }
                }
                
                scrollViewer.Content = columnsPanel;
                Grid.SetRow(scrollViewer, 2);
                grid.Children.Add(scrollViewer);
                
                // Add search functionality
                searchBox.TextChanged += (s, e) => {
                    FilterColumns(columnsPanel, searchBox.Text);
                };
                
                // Add the grid to the tab item
                tabItem.Content = grid;
                
                // Add the tab item to the TabControl
                ColumnsTabControl.Items.Add(tabItem);
            }
        }

        private void FilterColumns(StackPanel columnsPanel, string searchText)
        {
            if (string.IsNullOrWhiteSpace(searchText))
            {
                // Show all columns
                foreach (var child in columnsPanel.Children)
                {
                    if (child is FrameworkElement element)
                    {
                        element.Visibility = Visibility.Visible;
                    }
                }
            }
            else
            {
                // Case-insensitive search
                searchText = searchText.ToLower();
                
                // Filter columns based on search text
                foreach (var child in columnsPanel.Children)
                {
                    if (child is CheckBox checkbox && checkbox.Content is string columnName)
                    {
                        checkbox.Visibility = columnName.ToLower().Contains(searchText)
                            ? Visibility.Visible
                            : Visibility.Collapsed;
                    }
                }
            }
        }

        private void SelectVisibleColumns(string tableName, bool selected)
        {
            if (!selectedColumns.ContainsKey(tableName)) return;
            
            // Find the TabItem for this table
            foreach (TabItem tabItem in ColumnsTabControl.Items)
            {
                if (tabItem.Header.ToString() == tableName)
                {
                    // Find the ScrollViewer and its StackPanel
                    if (tabItem.Content is Grid grid && 
                        grid.Children.Count > 2 && 
                        grid.Children[2] is ScrollViewer scrollViewer && 
                        scrollViewer.Content is StackPanel columnsPanel)
                    {
                        // Update only visible checkboxes in this panel
                        foreach (var child in columnsPanel.Children)
                        {
                            if (child is CheckBox checkbox && checkbox.Visibility == Visibility.Visible)
                            {
                                string? columnName = checkbox.Content?.ToString();
                                if (!string.IsNullOrEmpty(columnName))
                                {
                                    checkbox.IsChecked = selected;
                                    selectedColumns[tableName][columnName] = selected;
                                }
                            }
                        }
                    }
                    
                    break;
                }
            }
        }

        private void SelectAllColumns(string tableName, bool selected)
        {
            if (!selectedColumns.ContainsKey(tableName)) return;
            
            // Find the TabItem for this table
            foreach (TabItem tabItem in ColumnsTabControl.Items)
            {
                if (tabItem.Header.ToString() == tableName)
                {
                    // Find the ScrollViewer and its StackPanel
                    if (tabItem.Content is Grid grid && 
                        grid.Children.Count > 2 && 
                        grid.Children[2] is ScrollViewer scrollViewer && 
                        scrollViewer.Content is StackPanel columnsPanel)
                    {
                        // Update all checkboxes in this panel regardless of visibility
                        foreach (var child in columnsPanel.Children)
                        {
                            if (child is CheckBox checkbox)
                            {
                                string? columnName = checkbox.Content?.ToString();
                                if (!string.IsNullOrEmpty(columnName))
                                {
                                    checkbox.IsChecked = selected;
                                    selectedColumns[tableName][columnName] = selected;
                                }
                            }
                        }
                    }
                    
                    break;
                }
            }
        }

        private void ContinueToOperationsButton_Click(object sender, RoutedEventArgs e)
        {
            if (selectedTables.Count == 0)
            {
                MessageBox.Show("Please select at least one table first.", 
                    "No Tables Selected", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            // Move to operations tab
            MainTabControl.SelectedIndex = 4;
        }

        private void AddOrderByColumn_Click(object sender, RoutedEventArgs e)
        {
            // Create a new order by entry
            var entry = new OrderByEntry();
            orderByEntries.Add(entry);
            
            // Create a panel to hold the entry controls
            var entryPanel = new StackPanel
            {
                Orientation = Orientation.Horizontal,
                Margin = new Thickness(5)
            };
            
            // Create column selection combo box
            var columnComboBox = new ComboBox
            {
                MinWidth = 200,
                Margin = new Thickness(5),
                IsEditable = true,
                StaysOpenOnEdit = true
            };
            
            // Populate with all available columns
            foreach (var column in GetAllAvailableColumns())
            {
                columnComboBox.Items.Add(column);
            }
            
            // Create direction combo box (ASC/DESC)
            var directionComboBox = new ComboBox
            {
                MinWidth = 80,
                Margin = new Thickness(5)
            };
            
            // Add sort directions
            foreach (var order in SORT_ORDERS)
            {
                directionComboBox.Items.Add(order);
            }
            
            // Default to ASC
            directionComboBox.SelectedIndex = 0;
            
            // Store references to the controls
            entry.ColumnComboBox = columnComboBox;
            entry.OrderComboBox = directionComboBox;
            
            // Create remove button
            var removeButton = new Button
            {
                Content = "X",
                Width = 25,
                Height = 25,
                Margin = new Thickness(5),
                ToolTip = "Remove this entry"
            };
            
            // Handle remove button click
            removeButton.Click += (s, args) =>
            {
                // Remove the entry from the list
                orderByEntries.Remove(entry);
                
                // Remove the entry panel from the parent
                if (entryPanel.Parent is StackPanel parent)
                {
                    parent.Children.Remove(entryPanel);
                }
            };
            
            // Add controls to the entry panel
            entryPanel.Children.Add(new TextBlock 
            { 
                Text = "Column:", 
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(5)
            });
            entryPanel.Children.Add(columnComboBox);
            
            entryPanel.Children.Add(new TextBlock 
            { 
                Text = "Order:", 
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(5)
            });
            entryPanel.Children.Add(directionComboBox);
            
            entryPanel.Children.Add(removeButton);
            
            // Add the entry panel to the OrderByPanel
            // Insert before the "Add Order By Column" button (at index 0)
            OrderByPanel.Children.Insert(OrderByPanel.Children.Count > 0 ? 1 : 0, entryPanel);
        }

        private void AddGroupByColumn_Click(object sender, RoutedEventArgs e)
        {
            // Create a new group by entry
            var entry = new GroupByEntry();
            groupByEntries.Add(entry);
            
            // Create a panel to hold the entry controls
            var entryPanel = new StackPanel
            {
                Orientation = Orientation.Horizontal,
                Margin = new Thickness(5)
            };
            
            // Create column selection combo box
            var columnComboBox = new ComboBox
            {
                MinWidth = 200,
                Margin = new Thickness(5),
                IsEditable = true,
                StaysOpenOnEdit = true
            };
            
            // Populate with all available columns
            foreach (var column in GetAllAvailableColumns())
            {
                columnComboBox.Items.Add(column);
            }
            
            // Store reference to the control
            entry.ColumnComboBox = columnComboBox;
            
            // Create remove button
            var removeButton = new Button
            {
                Content = "X",
                Width = 25,
                Height = 25,
                Margin = new Thickness(5),
                ToolTip = "Remove this entry"
            };
            
            // Handle remove button click
            removeButton.Click += (s, args) =>
            {
                // Remove the entry from the list
                groupByEntries.Remove(entry);
                
                // Remove the entry panel from the parent
                if (entryPanel.Parent is StackPanel parent)
                {
                    parent.Children.Remove(entryPanel);
                }
            };
            
            // Add controls to the entry panel
            entryPanel.Children.Add(new TextBlock 
            { 
                Text = "Column:", 
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(5)
            });
            entryPanel.Children.Add(columnComboBox);
            entryPanel.Children.Add(removeButton);
            
            // Add the entry panel to the GroupByPanel
            // Insert before the "Add Group By Column" button (at index 0)
            GroupByPanel.Children.Insert(GroupByPanel.Children.Count > 0 ? 1 : 0, entryPanel);
        }

        private void AddAggregateFunction_Click(object sender, RoutedEventArgs e)
        {
            // Create a new aggregate function entry
            var entry = new AggregateEntry();
            aggregateEntries.Add(entry);
            
            // Create a panel to hold the entry controls
            var entryPanel = new StackPanel
            {
                Orientation = Orientation.Horizontal,
                Margin = new Thickness(5)
            };
            
            // Create function selection combo box
            var functionComboBox = new ComboBox
            {
                MinWidth = 120,
                Margin = new Thickness(5)
            };
            
            // Add aggregate functions
            foreach (var function in AGGREGATE_FUNCTIONS)
            {
                functionComboBox.Items.Add(function);
            }
            
            // Default to COUNT
            functionComboBox.SelectedIndex = 0;
            
            // Create column selection combo box
            var columnComboBox = new ComboBox
            {
                MinWidth = 200,
                Margin = new Thickness(5),
                IsEditable = true,
                StaysOpenOnEdit = true
            };
            
            // Populate with all available columns
            foreach (var column in GetAllAvailableColumns())
            {
                columnComboBox.Items.Add(column);
            }
            
            // Create alias text box
            var aliasTextBox = new TextBox
            {
                MinWidth = 150,
                Margin = new Thickness(5),
                ToolTip = "Custom name for this column in the results"
            };
            
            // Store references to the controls
            entry.FunctionComboBox = functionComboBox;
            entry.ColumnComboBox = columnComboBox;
            entry.AliasTextBox = aliasTextBox;
            
            // Add event handlers to suggest alias
            functionComboBox.SelectionChanged += (s, args) => UpdateAggregateAliasSuggestion(entry);
            columnComboBox.SelectionChanged += (s, args) => UpdateAggregateAliasSuggestion(entry);
            
            // Create remove button
            var removeButton = new Button
            {
                Content = "X",
                Width = 25,
                Height = 25,
                Margin = new Thickness(5),
                ToolTip = "Remove this entry"
            };
            
            // Handle remove button click
            removeButton.Click += (s, args) =>
            {
                // Remove the entry from the list
                aggregateEntries.Remove(entry);
                
                // Remove the entry panel from the parent
                if (entryPanel.Parent is StackPanel parent)
                {
                    parent.Children.Remove(entryPanel);
                }
            };
            
            // Add controls to the entry panel
            entryPanel.Children.Add(new TextBlock 
            { 
                Text = "Function:", 
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(5)
            });
            entryPanel.Children.Add(functionComboBox);
            
            entryPanel.Children.Add(new TextBlock 
            { 
                Text = "Column:", 
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(5)
            });
            entryPanel.Children.Add(columnComboBox);
            
            entryPanel.Children.Add(new TextBlock 
            { 
                Text = "Alias:", 
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(5)
            });
            entryPanel.Children.Add(aliasTextBox);
            
            entryPanel.Children.Add(removeButton);
            
            // Add the entry panel to the AggregatePanel
            // Insert before the "Add Aggregate Function" button (at index 0)
            AggregatePanel.Children.Insert(AggregatePanel.Children.Count > 0 ? 1 : 0, entryPanel);
            
            // Generate initial alias suggestion
            UpdateAggregateAliasSuggestion(entry);
        }

        private void UpdateAggregateAliasSuggestion(AggregateEntry entry)
        {
            try
            {
                if (entry.FunctionComboBox == null || entry.ColumnComboBox == null || entry.AliasTextBox == null)
                {
                    return; // One of the controls is null, can't proceed
                }
                
                string function = entry.FunctionComboBox.SelectedItem as string ?? string.Empty;
                string column = entry.ColumnComboBox.SelectedItem as string ?? string.Empty;
                string currentAlias = entry.AliasTextBox.Text;
                
                // Only generate a suggestion if the alias field is empty or hasn't been manually edited
                if (string.IsNullOrWhiteSpace(currentAlias) || IsAutoGeneratedAlias(currentAlias, function, column))
                {
                    if (!string.IsNullOrEmpty(function) && !string.IsNullOrEmpty(column))
                    {
                        // Extract column name without table prefix
                        string columnName = column;
                        if (column.Contains("."))
                        {
                            string[] parts = column.Split('.');
                            if (parts.Length == 2)
                            {
                                columnName = parts[1]; // Get the column name part
                            }
                        }
                        
                        // Generate a suggested alias - combine function name with column name
                        string suggestedAlias = $"{function}_{columnName}";
                        
                        // Update the alias field
                        entry.AliasTextBox.Text = suggestedAlias;
                        
                        // Optionally offer to deselect the original column to avoid duplication in the results
                        OfferToDeselectOriginalColumn(column);
                    }
                }
            }
            catch (Exception ex)
            {
                // Handle any exceptions that might occur
                MessageBox.Show($"Error generating alias suggestion: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        
        /// <summary>
        /// Determine if the current alias was auto-generated (to avoid overwriting user edits)
        /// </summary>
        private bool IsAutoGeneratedAlias(string alias, string function, string column)
        {
            // Extract column name from fully qualified name
            string columnName = column;
            if (column.Contains("."))
            {
                string[] parts = column.Split('.');
                if (parts.Length == 2)
                {
                    columnName = parts[1];
                }
            }
            
            // Check if the alias matches any of our auto-generation patterns
            string expectedAlias = $"{function}_{columnName}";
            return alias == expectedAlias;
        }
        
        /// <summary>
        /// Offers to deselect the original column when it's used in an aggregate function
        /// </summary>
        private void OfferToDeselectOriginalColumn(string column)
        {
            // Only consider fully qualified column names (table.column)
            if (!column.Contains("."))
                return;
                
            string[] parts = column.Split('.');
            if (parts.Length != 2)
                return;
                
            string tableName = parts[0];
            string columnName = parts[1];
            
            // Check if this column is currently selected in the columns tab
            if (selectedColumns.ContainsKey(tableName) && 
                selectedColumns[tableName].ContainsKey(columnName) && 
                selectedColumns[tableName][columnName])
            {
                // Ask user if they want to deselect the original column
                var result = MessageBox.Show(
                    $"Would you like to remove {column} from the regular columns selection?\n\n" +
                    "This avoids duplicating the same data in your query results.",
                    "Remove Duplicate Column",
                    MessageBoxButton.YesNo,
                    MessageBoxImage.Question);
                    
                if (result == MessageBoxResult.Yes)
                {
                    // Find the checkbox for this column
                    foreach (TabItem tabItem in ColumnsTabControl.Items)
                    {
                        if (tabItem.Header.ToString() == tableName)
                        {
                            if (tabItem.Content is Grid grid && 
                                grid.Children.Count > 2 && 
                                grid.Children[2] is ScrollViewer scrollViewer && 
                                scrollViewer.Content is StackPanel columnsPanel)
                            {
                                foreach (var item in columnsPanel.Children)
                                {
                                    if (item is CheckBox checkBox && checkBox.Content.ToString() == columnName)
                                    {
                                        // Deselect the checkbox
                                        checkBox.IsChecked = false;
                                        selectedColumns[tableName][columnName] = false;
                                        break;
                                    }
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }

        private void AddWhereCondition_Click(object sender, RoutedEventArgs e)
        {
            // Create a new where condition entry
            var entry = new WhereEntry();
            whereEntries.Add(entry);
            
            // Create a panel to hold the entry controls
            var entryPanel = new StackPanel
            {
                Orientation = Orientation.Horizontal,
                Margin = new Thickness(5)
            };
            
            // Create column selection combo box
            var columnComboBox = new ComboBox
            {
                MinWidth = 180,
                Margin = new Thickness(5),
                IsEditable = true,
                StaysOpenOnEdit = true
            };
            
            // Populate with all available columns
            foreach (var column in GetAllAvailableColumns())
            {
                columnComboBox.Items.Add(column);
            }
            
            // Create operator combo box
            var operatorComboBox = new ComboBox
            {
                MinWidth = 100,
                Margin = new Thickness(5)
            };
            
            // Add operators
            string[] operators = { "=", "<>", ">", "<", ">=", "<=", "LIKE", "IN", "IS NULL", "IS NOT NULL", "BETWEEN" };
            foreach (var op in operators)
            {
                operatorComboBox.Items.Add(op);
            }
            
            // Default to equals
            operatorComboBox.SelectedIndex = 0;
            
            // Create value text box
            var valueTextBox = new TextBox
            {
                MinWidth = 150,
                Margin = new Thickness(5)
            };
            
            // Create connector combo box (AND/OR)
            var connectorComboBox = new ComboBox
            {
                MinWidth = 80,
                Margin = new Thickness(5)
            };
            
            // Add connectors
            connectorComboBox.Items.Add("AND");
            connectorComboBox.Items.Add("OR");
            
            // Default to AND
            connectorComboBox.SelectedIndex = 0;
            
            // Store references to the controls
            entry.ColumnComboBox = columnComboBox;
            entry.OperatorComboBox = operatorComboBox;
            entry.ValueTextBox = valueTextBox;
            entry.ConnectorComboBox = connectorComboBox;
            
            // Enable/disable value text box based on operator
            operatorComboBox.SelectionChanged += (s, args) =>
            {
                string op = operatorComboBox.SelectedItem as string ?? string.Empty;
                valueTextBox.IsEnabled = !(op == "IS NULL" || op == "IS NOT NULL");
                
                // Add placeholder text based on the operator
                if (op == "IN")
                {
                    valueTextBox.Text = "value1, value2, value3";
                }
                else if (op == "BETWEEN")
                {
                    valueTextBox.Text = "lower_value AND upper_value";
                }
                else if (op == "LIKE")
                {
                    valueTextBox.Text = "%value%";
                }
                else if (valueTextBox.IsEnabled && string.IsNullOrWhiteSpace(valueTextBox.Text))
                {
                    valueTextBox.Text = "";
                }
            };
            
            // Create remove button
            var removeButton = new Button
            {
                Content = "X",
                Width = 25,
                Height = 25,
                Margin = new Thickness(5),
                ToolTip = "Remove this condition"
            };
            
            // Handle remove button click
            removeButton.Click += (s, args) =>
            {
                // Remove the entry from the list
                whereEntries.Remove(entry);
                
                // Remove the entry panel from the parent
                if (entryPanel.Parent is StackPanel parent)
                {
                    parent.Children.Remove(entryPanel);
                }
                
                // Hide connector combo box for last entry
                UpdateWhereConnectors();
            };
            
            // Add controls to the entry panel
            entryPanel.Children.Add(new TextBlock 
            { 
                Text = "Column:", 
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(5)
            });
            entryPanel.Children.Add(columnComboBox);
            
            entryPanel.Children.Add(new TextBlock 
            { 
                Text = "Operator:", 
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(5)
            });
            entryPanel.Children.Add(operatorComboBox);
            
            entryPanel.Children.Add(new TextBlock 
            { 
                Text = "Value:", 
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(5)
            });
            entryPanel.Children.Add(valueTextBox);
            
            entryPanel.Children.Add(new TextBlock 
            { 
                Text = "Next:", 
                VerticalAlignment = VerticalAlignment.Center,
                Margin = new Thickness(5)
            });
            entryPanel.Children.Add(connectorComboBox);
            
            entryPanel.Children.Add(removeButton);
            
            // Add the entry panel to the WherePanel
            // Insert before the "Add Where Condition" button
            WherePanel.Children.Insert(WherePanel.Children.Count > 0 ? 1 : 0, entryPanel);
            
            // Update connector visibility
            UpdateWhereConnectors();
        }
        
        private void UpdateWhereConnectors()
        {
            // Hide the connector dropdown for the last WHERE condition
            for (int i = 0; i < whereEntries.Count; i++)
            {
                var entry = whereEntries[i];
                if (entry.ConnectorComboBox != null)
                {
                    // Only show connector for conditions that aren't the last one
                    bool isLastCondition = (i == whereEntries.Count - 1);
                    
                    // Find the connector and its label
                    if (entry.ConnectorComboBox.Parent is StackPanel panel)
                    {
                        int connectorIndex = panel.Children.IndexOf(entry.ConnectorComboBox);
                        
                        if (connectorIndex > 0 && connectorIndex - 1 < panel.Children.Count)
                        {
                            var connectorLabel = panel.Children[connectorIndex - 1];
                            
                            // Set visibility
                            entry.ConnectorComboBox.Visibility = isLastCondition ? Visibility.Collapsed : Visibility.Visible;
                            connectorLabel.Visibility = isLastCondition ? Visibility.Collapsed : Visibility.Visible;
                        }
                    }
                }
            }
        }

        private void AddCombinedColumn_Click(object sender, RoutedEventArgs e)
        {
            // Create a new combined column entry
            var entry = new CombinedColumnEntry();
            combinedColumnEntries.Add(entry);
            
            // Create a main GroupBox to hold all the combined column elements
            var groupBox = new GroupBox
            {
                Header = "Combined Column",
                Margin = new Thickness(5, 10, 5, 15),
                Padding = new Thickness(10)
            };
            
            // Create a StackPanel to hold all components inside the GroupBox
            var mainPanel = new StackPanel();
            groupBox.Content = mainPanel;
            
            // Create alias section
            var aliasPanel = new DockPanel { Margin = new Thickness(0, 5, 0, 10) };
            
            var aliasLabel = new Label
            {
                Content = "Output column alias:",
                VerticalAlignment = VerticalAlignment.Center
            };
            
            var aliasTextBox = new TextBox
            {
                Width = 200,
                Margin = new Thickness(5, 0, 5, 0),
                VerticalAlignment = VerticalAlignment.Center
            };
            
            // Store reference to the alias TextBox
            entry.AliasTextBox = aliasTextBox;
            
            var suggestButton = new Button
            {
                Content = "Suggest Alias",
                Padding = new Thickness(5, 2, 5, 2),
                Margin = new Thickness(5, 0, 0, 0),
                VerticalAlignment = VerticalAlignment.Center
            };
            
            // Hook up the suggest alias button
            suggestButton.Click += (s, args) => SuggestCombinedAlias(entry);
            
            // Add controls to the alias panel
            aliasPanel.Children.Add(aliasLabel);
            aliasPanel.Children.Add(aliasTextBox);
            aliasPanel.Children.Add(suggestButton);
            
            mainPanel.Children.Add(aliasPanel);
            
            // Add separator
            mainPanel.Children.Add(new Separator { Margin = new Thickness(0, 5, 0, 5) });
            
            // Create columns selection section
            var columnsPanel = new StackPanel { Margin = new Thickness(0, 5, 0, 10) };
            
            var columnsLabel = new TextBlock
            {
                Text = "Select columns to combine:",
                Margin = new Thickness(5, 0, 0, 5)
            };
            columnsPanel.Children.Add(columnsLabel);
            
            // Create filter section for column search
            var filterPanel = new DockPanel { Margin = new Thickness(0, 5, 0, 10) };
            
            var filterLabel = new Label
            {
                Content = "Filter by column name:",
                VerticalAlignment = VerticalAlignment.Center
            };
            
            var filterTextBox = new TextBox
            {
                Width = 200,
                Margin = new Thickness(5, 0, 5, 0),
                VerticalAlignment = VerticalAlignment.Center
            };
            
            filterPanel.Children.Add(filterLabel);
            filterPanel.Children.Add(filterTextBox);
            
            columnsPanel.Children.Add(filterPanel);
            
            // Create a ScrollViewer for the checkboxes
            var scrollViewer = new ScrollViewer
            {
                MaxHeight = 150,
                VerticalScrollBarVisibility = ScrollBarVisibility.Auto,
                HorizontalScrollBarVisibility = ScrollBarVisibility.Auto,
                Margin = new Thickness(5, 0, 5, 5)
            };
            
            // Create a StackPanel for the checkboxes
            var checkboxesPanel = new StackPanel();
            entry.CheckboxesPanel = checkboxesPanel;
            scrollViewer.Content = checkboxesPanel;
            
            // Add checkboxes for all available columns
            entry.ColumnCheckboxes = new Dictionary<string, CheckBox>();
            foreach (var column in GetAllAvailableColumns())
            {
                var checkbox = new CheckBox
                {
                    Content = column,
                    Margin = new Thickness(5, 2, 5, 2)
                };
                
                entry.ColumnCheckboxes[column] = checkbox;
                checkboxesPanel.Children.Add(checkbox);
            }
            
            // Add filter functionality
            filterTextBox.TextChanged += (s, args) =>
            {
                var filter = filterTextBox.Text.Trim().ToLower();
                
                foreach (var kvp in entry.ColumnCheckboxes)
                {
                    var columnName = kvp.Key.ToLower();
                    var checkbox = kvp.Value;
                    
                    checkbox.Visibility = string.IsNullOrEmpty(filter) || columnName.Contains(filter) 
                        ? Visibility.Visible 
                        : Visibility.Collapsed;
                }
            };
            
            columnsPanel.Children.Add(scrollViewer);
            mainPanel.Children.Add(columnsPanel);
            
            // Add separator
            mainPanel.Children.Add(new Separator { Margin = new Thickness(0, 5, 0, 5) });
            
            // Create remove button
            var removeButton = new Button
            {
                Content = "Remove this Combined Column",
                Margin = new Thickness(0, 5, 0, 0),
                HorizontalAlignment = HorizontalAlignment.Center
            };
            
            // Hook up the remove button
            removeButton.Click += (s, args) =>
            {
                // Remove the entry from the list
                combinedColumnEntries.Remove(entry);
                
                // Remove the GroupBox from the parent
                if (groupBox.Parent is StackPanel parent)
                {
                    parent.Children.Remove(groupBox);
                }
            };
            
            mainPanel.Children.Add(removeButton);
            
            // Add the GroupBox to the CombinedColumnsPanel, but insert it before the "Add" button
            // which should be the first item in the panel
            int insertIndex = 0;
            foreach (var child in CombinedColumnsPanel.Children)
            {
                if (child is TextBlock)
                {
                    insertIndex++;
                }
                else if (child is Button button && button.Content.ToString() == "Add New Combined Column")
                {
                    insertIndex++;
                    break;
                }
            }
            
            CombinedColumnsPanel.Children.Insert(insertIndex, groupBox);
        }

        private void SuggestCombinedAlias(CombinedColumnEntry entry)
        {
            if (entry.ColumnCheckboxes == null || entry.AliasTextBox == null)
                return;
                
            // Get selected columns from checkboxes
            var selectedColumns = new List<string>();
            
            foreach (var kvp in entry.ColumnCheckboxes)
            {
                var columnName = kvp.Key;
                var checkbox = kvp.Value;
                
                if (checkbox.IsChecked == true)
                {
                    selectedColumns.Add(columnName);
                }
            }
            
            if (selectedColumns.Count == 0)
                return;
                
            // Extract just the column names without table prefixes
            var commonNames = new HashSet<string>();
            
            foreach (var column in selectedColumns)
            {
                if (column.Contains("."))
                {
                    // Extract just the column name without the table prefix
                    string[] parts = column.Split('.');
                    if (parts.Length == 2)
                    {
                        string columnName = parts[1];
                        commonNames.Add(columnName);
                    }
                }
            }
            
            if (commonNames.Count == 0)
                return;
                
            // Generate a suggested alias
            string suggestedAlias;
            if (commonNames.Count == 1)
            {
                // If all selected columns share the same name, use that
                suggestedAlias = commonNames.First();
            }
            else
            {
                // Otherwise, create a combined name
                suggestedAlias = "combined_" + string.Join("_", commonNames);
            }
            
            // Update the alias text box
            entry.AliasTextBox.Text = suggestedAlias;
        }

        private void GenerateSqlButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                string sql = GenerateSqlQuery();
                QueryTextBox.Text = sql;
                
                // Move to next tab
                MainTabControl.SelectedIndex = 5;
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error generating SQL query: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private string GenerateSqlQuery()
        {
            if (selectedTables.Count == 0)
            {
                throw new Exception("No tables selected.");
            }

            var sql = new StringBuilder();
            sql.AppendLine("SELECT");

            // Process selected columns
            var columnsList = new List<string>();
            
            // Add regular columns
            foreach (var tableName in selectedTables)
            {
                if (selectedColumns.TryGetValue(tableName, out var columnsDict))
                {
                    foreach (var kvp in columnsDict)
                    {
                        string columnName = kvp.Key;
                        bool isSelected = kvp.Value;
                        
                        if (isSelected)
                        {
                            columnsList.Add($"{tableName}.{columnName}");
                        }
                    }
                }
            }

            // Add aggregate functions
            foreach (var agg in aggregateEntries)
            {
                if (agg.FunctionComboBox?.SelectedItem == null || string.IsNullOrEmpty(agg.ColumnComboBox?.SelectedItem?.ToString()))
                    continue;
                
                string function = agg.FunctionComboBox.SelectedItem.ToString() ?? "";
                string column = agg.ColumnComboBox.SelectedItem.ToString() ?? "";
                string alias = agg.AliasTextBox?.Text ?? "";
                
                string aggExpr = $"{function}({column})";
                if (!string.IsNullOrEmpty(alias))
                {
                    aggExpr += $" AS {alias}";
                }
                
                columnsList.Add(aggExpr);
            }
            
            // Add combined columns
            foreach (var combined in combinedColumnEntries)
            {
                if (combined.AliasTextBox == null || string.IsNullOrEmpty(combined.AliasTextBox.Text))
                    continue;
                
                var selectedCols = new List<string>();
                if (combined.ColumnCheckboxes != null)
                {
                    foreach (var kvp in combined.ColumnCheckboxes)
                    {
                        if (kvp.Value.IsChecked == true)
                        {
                            selectedCols.Add(kvp.Key);
                        }
                    }
                }
                
                if (selectedCols.Count == 0)
                    continue;
                
                string alias = combined.AliasTextBox.Text;
                string coalesceExpr = $"COALESCE({string.Join(", ", selectedCols)}) AS {alias}";
                columnsList.Add(coalesceExpr);
            }
            
            // If no columns are selected, use *
            if (columnsList.Count == 0)
            {
                sql.AppendLine("  *");
            }
            else
            {
                sql.AppendLine("  " + string.Join(",\n  ", columnsList));
            }
            
            // Process FROM clause with JOIN statements
            if (joinEntries.Count > 0)
            {
                // Take the left table of the first join as the main table
                JoinEntry firstJoin = joinEntries[0];
                string mainTable = firstJoin.LeftTableComboBox?.SelectedItem?.ToString() ?? selectedTables[0];
                
                sql.AppendLine($"FROM {mainTable}");
                
                // Add each join
                foreach (var join in joinEntries)
                {
                    if (join.JoinTypeComboBox == null || join.RightTableComboBox == null)
                        continue;
                    
                    string joinType = join.JoinTypeComboBox.SelectedItem?.ToString() ?? "INNER JOIN";
                    string rightTable = join.RightTableComboBox.SelectedItem?.ToString() ?? "";
                    
                    if (string.IsNullOrEmpty(rightTable))
                        continue;
                    
                    sql.Append($"{joinType} {rightTable}");
                    
                    // Add ON condition if join type requires it 
                    bool needsOnClause = joinType != "CROSS JOIN";
                    
                    if (needsOnClause)
                    {
                        // Make sure we have valid column selections
                        if (join.LeftColumnComboBox != null && join.RightColumnComboBox != null)
                        {
                            // Get the selected column values
                            object leftColObj = join.LeftColumnComboBox.SelectedItem;
                            object rightColObj = join.RightColumnComboBox.SelectedItem;
                            
                            // Convert to strings with null checks
                            string leftCol = leftColObj?.ToString() ?? "";
                            string rightCol = rightColObj?.ToString() ?? "";
                            
                            // If either column is empty, try to use text content if the combobox is editable
                            if (string.IsNullOrEmpty(leftCol) && join.LeftColumnComboBox.IsEditable)
                            {
                                leftCol = join.LeftColumnComboBox.Text;
                            }
                            
                            if (string.IsNullOrEmpty(rightCol) && join.RightColumnComboBox.IsEditable)
                            {
                                rightCol = join.RightColumnComboBox.Text;
                            }
                            
                            // Only add the ON clause if we have both column values
                            if (!string.IsNullOrEmpty(leftCol) && !string.IsNullOrEmpty(rightCol))
                            {
                                sql.AppendLine($" ON {leftCol} = {rightCol}");
                            }
                            else
                            {
                                // Default to a placeholder if columns weren't properly selected
                                string leftTable = join.LeftTableComboBox?.SelectedItem?.ToString() ?? "";
                                
                                if (!string.IsNullOrEmpty(leftTable) && !string.IsNullOrEmpty(rightTable))
                                {
                                    // Try to use common column names like 'id' as fallback
                                    sql.AppendLine($" ON {leftTable}.id = {rightTable}.id");
                                }
                                else
                                {
                                    sql.AppendLine(); // Just add a newline as last resort
                                }
                            }
                        }
                        else
                        {
                            sql.AppendLine(); // Just add a newline if components are missing
                        }
                    }
                    else
                    {
                        sql.AppendLine(); // Add a newline for CROSS JOIN
                    }
                }
            }
            else
            {
                // If no joins are configured, just use the first selected table
                sql.AppendLine($"FROM {selectedTables[0]}");
            }
            
            // WHERE clause
            if (whereEntries.Count > 0)
            {
                sql.Append("WHERE ");
                
                for (int i = 0; i < whereEntries.Count; i++)
                {
                    var where = whereEntries[i];
                    if (where.ColumnComboBox == null || where.OperatorComboBox == null)
                        continue;
                    
                    string column = where.ColumnComboBox.SelectedItem?.ToString() ?? "";
                    string op = where.OperatorComboBox.SelectedItem?.ToString() ?? "";
                    string val = where.ValueTextBox?.Text ?? "";
                    
                    if (string.IsNullOrEmpty(column) || string.IsNullOrEmpty(op))
                        continue;
                    
                    if (i > 0)
                    {
                        string connector = where.ConnectorComboBox?.SelectedItem?.ToString() ?? "AND";
                        sql.Append($" {connector} ");
                    }
                    
                    if (op == "IS NULL" || op == "IS NOT NULL")
                    {
                        sql.Append($"{column} {op}");
                    }
                    else
                    {
                        // Add quotes for string values unless it's a numeric value
                        bool isNumeric = decimal.TryParse(val, out _);
                        string formattedValue = isNumeric ? val : $"'{val}'";
                        
                        // BETWEEN and IN require special handling
                        if (op == "BETWEEN")
                        {
                            sql.Append($"{column} {op} {val}");
                        }
                        else if (op == "IN")
                        {
                            // Format the IN clause values with quotes for non-numeric values
                            var inValues = val.Split(',')
                                            .Select(v => v.Trim())
                                            .Select(v => decimal.TryParse(v, out _) ? v : $"'{v}'");
                            
                            sql.Append($"{column} {op} ({string.Join(", ", inValues)})");
                        }
                        else
                        {
                            // Standard operators
                            sql.Append($"{column} {op} {formattedValue}");
                        }
                    }
                }
                
                sql.AppendLine();
            }
            
            // GROUP BY clause
            if (groupByEntries.Count > 0)
            {
                var groupByCols = new List<string>();
                
                foreach (var entry in groupByEntries)
                {
                    if (entry.ColumnComboBox?.SelectedItem != null)
                    {
                        string column = entry.ColumnComboBox.SelectedItem.ToString() ?? "";
                        if (!string.IsNullOrEmpty(column))
                        {
                            groupByCols.Add(column);
                        }
                    }
                }
                
                if (groupByCols.Count > 0)
                {
                    sql.AppendLine($"GROUP BY {string.Join(", ", groupByCols)}");
                }
            }
            
            // ORDER BY clause
            if (orderByEntries.Count > 0)
            {
                var orderByCols = new List<string>();
                
                foreach (var entry in orderByEntries)
                {
                    if (entry.ColumnComboBox?.SelectedItem != null && entry.OrderComboBox?.SelectedItem != null)
                    {
                        string column = entry.ColumnComboBox.SelectedItem.ToString() ?? "";
                        string order = entry.OrderComboBox.SelectedItem.ToString() ?? "";
                        
                        if (!string.IsNullOrEmpty(column) && !string.IsNullOrEmpty(order))
                        {
                            orderByCols.Add($"{column} {order}");
                        }
                    }
                }
                
                if (orderByCols.Count > 0)
                {
                    sql.AppendLine($"ORDER BY {string.Join(", ", orderByCols)}");
                }
            }
            
            return sql.ToString();
        }

        private List<string> GetAllAvailableColumns()
        {
            List<string> allColumns = new List<string>();
            foreach (var tableName in selectedTables)
            {
                if (tables.ContainsKey(tableName))
                {
                    foreach (var column in tables[tableName])
                    {
                        allColumns.Add($"{tableName}.{column}");
                    }
                }
            }
            return allColumns;
        }

        private void CopyToClipboard_Click(object sender, RoutedEventArgs e)
        {
            if (!string.IsNullOrEmpty(QueryTextBox.Text))
            {
                Clipboard.SetText(QueryTextBox.Text);
                MessageBox.Show("SQL query copied to clipboard!", "Success", MessageBoxButton.OK, MessageBoxImage.Information);
            }
        }
        
        private async void ExecuteQuery_Click(object sender, RoutedEventArgs e)
        {
            string sql = QueryTextBox.Text.Trim();
            
            if (string.IsNullOrEmpty(sql))
            {
                MessageBox.Show("Please generate a SQL query first!", "Empty Query", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
            
            if (activeConn == null || activeCommand == null)
            {
                MessageBox.Show("Please connect to a database first!", "No Connection", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
            
            try
            {
                // Reset status
                StatusTextBlock.Text = "Executing query...";
                RowsTextBlock.Text = "0";
                TimeTextBlock.Text = "0 ms";
                
                // Allow UI to update
                await Dispatcher.InvokeAsync(() => { }, DispatcherPriority.Render);
                
                // Clear previous results
                ResultsDataGrid.ItemsSource = null;
                resultData = null;
                
                // Start timing
                var stopwatch = Stopwatch.StartNew();
                
                // Execute query
                activeCommand.CommandText = sql;
                
                using (var reader = activeCommand.ExecuteReader())
                {
                    // Get column names
                    var schemaTable = reader.GetSchemaTable();
                    var columnNames = new List<string>();
                    
                    if (schemaTable != null)
                    {
                        foreach (DataRow row in schemaTable.Rows)
                        {
                            string? columnName = row["ColumnName"]?.ToString();
                            if (!string.IsNullOrEmpty(columnName))
                            {
                                columnNames.Add(columnName);
                            }
                        }
                    }
                    
                    // Check if result set is very large
                    if (columnNames.Count > 100)
                    {
                        var result = MessageBox.Show(
                            $"This query returns {columnNames.Count} columns which may cause the application to slow down.\n\n" +
                            "Do you want to continue loading all columns?",
                            "Large Result Set",
                            MessageBoxButton.YesNo,
                            MessageBoxImage.Warning);
                            
                        if (result == MessageBoxResult.No)
                        {
                            StatusTextBlock.Text = "Query canceled - too many columns";
                            stopwatch.Stop();
                            return;
                        }
                    }
                    
                    // Create DataTable to hold results
                    resultData = new DataTable();
                    originalColumnOrder = new List<string>(columnNames);
                    
                    // Add columns to DataTable
                    foreach (var columnName in columnNames)
                    {
                        if (columnName != null) // Null check
                        {
                            resultData.Columns.Add(columnName);
                        }
                    }
                    
                    // Fetch rows in batches
                    int rowCount = 0;
                    int batchSize = 1000;
                    
                    while (reader.Read())
                    {
                        var rowValues = new object[reader.FieldCount];
                        reader.GetValues(rowValues);
                        
                        resultData.Rows.Add(rowValues);
                        rowCount++;
                        
                        // Update status periodically
                        if (rowCount % batchSize == 0)
                        {
                            await Dispatcher.InvokeAsync(() =>
                            {
                                StatusTextBlock.Text = $"Fetching data... ({rowCount} rows)";
                                RowsTextBlock.Text = rowCount.ToString();
                            }, DispatcherPriority.Background);
                        }
                    }
                    
                    // Stop timing
                    stopwatch.Stop();
                    var executionTime = stopwatch.ElapsedMilliseconds;
                    
                    // Update UI with results
                    await Dispatcher.InvokeAsync(() =>
                    {
                        // Bind data to grid
                        ResultsDataGrid.ItemsSource = resultData.DefaultView;
                        
                        // Update status
                        StatusTextBlock.Text = "Query executed successfully";
                        RowsTextBlock.Text = rowCount.ToString();
                        TimeTextBlock.Text = $"{executionTime} ms";
                        
                        // Move to results tab
                        MainTabControl.SelectedIndex = 6; // Navigate to Results tab
                        
                        // Auto-adjust column widths
                        OptimizeColumnWidths_Click(null, null);
                        
                        // Show success message
                        MessageBox.Show($"Query executed successfully.\nRows returned: {rowCount}", 
                            "Success", MessageBoxButton.OK, MessageBoxImage.Information);
                    });
                }
            }
            catch (Exception ex)
            {
                StatusTextBlock.Text = $"Error: {ex.Message.Substring(0, Math.Min(50, ex.Message.Length))}...";
                MessageBox.Show($"Query execution failed: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
        
        private void OptimizeColumnWidths_Click(object sender, RoutedEventArgs e)
        {
            if (ResultsDataGrid.ItemsSource == null) return;
            
            try
            {
                Mouse.OverrideCursor = Cursors.Wait;
                StatusTextBlock.Text = "Optimizing column widths...";
                
                foreach (var column in ResultsDataGrid.Columns)
                {
                    // Set reasonable column width (between 50 and 300)
                    double width = Math.Min(300, Math.Max(50, column.ActualWidth * 1.1));
                    column.Width = new DataGridLength(width);
                }
                
                StatusTextBlock.Text = "Column widths optimized";
            }
            catch (Exception ex)
            {
                StatusTextBlock.Text = $"Error optimizing column widths: {ex.Message}";
            }
            finally
            {
                Mouse.OverrideCursor = null;
            }
        }
        
        private void ResetColumnOrder_Click(object sender, RoutedEventArgs e)
        {
            if (ResultsDataGrid.ItemsSource == null || originalColumnOrder == null || originalColumnOrder.Count == 0) return;
            
            try
            {
                Mouse.OverrideCursor = Cursors.Wait;
                StatusTextBlock.Text = "Resetting column order...";
                
                // Create a new view of the data with the original column order
                var view = new DataView(resultData!); // Use null-forgiving operator since we already checked resultData above
                
                ResultsDataGrid.ItemsSource = null;
                ResultsDataGrid.ItemsSource = view;
                
                // Ensure columns are in the original order
                for (int i = 0; i < originalColumnOrder.Count; i++)
                {
                    string originalCol = originalColumnOrder[i];
                    if (i < ResultsDataGrid.Columns.Count && 
                        ResultsDataGrid.Columns[i].Header?.ToString() != originalCol)
                    {
                        // Find the column and move it to the correct position
                        for (int j = i + 1; j < ResultsDataGrid.Columns.Count; j++)
                        {
                            if (ResultsDataGrid.Columns[j].Header?.ToString() == originalCol)
                            {
                                ResultsDataGrid.Columns.Move(j, i);
                                break;
                            }
                        }
                    }
                }
                
                StatusTextBlock.Text = "Column order reset to original";
            }
            catch (Exception ex)
            {
                StatusTextBlock.Text = $"Error resetting column order: {ex.Message}";
            }
            finally
            {
                Mouse.OverrideCursor = null;
            }
        }
        
        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (resultData == null || resultData.Rows.Count == 0)
            {
                MessageBox.Show("There is no data to export!", "No Data", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
            
            var saveFileDialog = new SaveFileDialog
            {
                Filter = "CSV files (*.csv)|*.csv|All files (*.*)|*.*",
                DefaultExt = "csv",
                AddExtension = true
            };
            
            if (saveFileDialog.ShowDialog() == true)
            {
                try
                {
                    Mouse.OverrideCursor = Cursors.Wait;
                    StatusTextBlock.Text = "Exporting to CSV...";
                    
                    using (var writer = new StreamWriter(saveFileDialog.FileName))
                    using (var csv = new CsvHelper.CsvWriter(writer, CultureInfo.InvariantCulture))
                    {
                        // Write headers
                        foreach (DataColumn column in resultData.Columns)
                        {
                            csv.WriteField(column.ColumnName);
                        }
                        csv.NextRecord();
                        
                        // Write data
                        for (int i = 0; i < resultData.Rows.Count; i++)
                        {
                            for (int j = 0; j < resultData.Columns.Count; j++)
                            {
                                csv.WriteField(resultData.Rows[i][j]);
                            }
                            csv.NextRecord();
                        }
                    }
                    
                    StatusTextBlock.Text = "Export complete";
                    MessageBox.Show($"Data exported to {saveFileDialog.FileName}", "Export Success", MessageBoxButton.OK, MessageBoxImage.Information);
                }
                catch (Exception ex)
                {
                    StatusTextBlock.Text = "Export failed";
                    MessageBox.Show($"Error exporting data: {ex.Message}", "Export Failed", MessageBoxButton.OK, MessageBoxImage.Error);
                }
                finally
                {
                    Mouse.OverrideCursor = null;
                }
            }
        }
        
        private void ExportToExcel_Click(object sender, RoutedEventArgs e)
        {
            if (resultData == null || resultData.Rows.Count == 0)
            {
                MessageBox.Show("There is no data to export!", "No Data", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }
            
            var saveFileDialog = new SaveFileDialog
            {
                Filter = "Excel files (*.xlsx)|*.xlsx|All files (*.*)|*.*",
                DefaultExt = "xlsx",
                AddExtension = true
            };
            
            if (saveFileDialog.ShowDialog() == true)
            {
                try
                {
                    Mouse.OverrideCursor = Cursors.Wait;
                    StatusTextBlock.Text = "Exporting to Excel...";
                    
                    using (var package = new ExcelPackage())
                    {
                        var worksheet = package.Workbook.Worksheets.Add("Query Results");
                        
                        // Add headers
                        for (int i = 0; i < resultData.Columns.Count; i++)
                        {
                            worksheet.Cells[1, i + 1].Value = resultData.Columns[i].ColumnName;
                            // Style header row
                            worksheet.Cells[1, i + 1].Style.Font.Bold = true;
                            worksheet.Cells[1, i + 1].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.Solid;
                            worksheet.Cells[1, i + 1].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                        }
                        
                        // Add data
                        for (int row = 0; row < resultData.Rows.Count; row++)
                        {
                            for (int col = 0; col < resultData.Columns.Count; col++)
                            {
                                var value = resultData.Rows[row][col];
                                worksheet.Cells[row + 2, col + 1].Value = value;
                            }
                            
                            // Update status for large exports
                            if (row % 1000 == 0 && row > 0)
                            {
                                Dispatcher.Invoke(() =>
                                {
                                    StatusTextBlock.Text = $"Exporting to Excel... ({row}/{resultData.Rows.Count} rows)";
                                });
                            }
                        }
                        
                        // Auto-fit columns
                        worksheet.Cells[worksheet.Dimension.Address].AutoFitColumns();
                        
                        // Save the file
                        var fileInfo = new FileInfo(saveFileDialog.FileName);
                        package.SaveAs(fileInfo);
                    }
                    
                    StatusTextBlock.Text = "Export complete";
                    MessageBox.Show($"Data exported to {saveFileDialog.FileName}", "Export Success", MessageBoxButton.OK, MessageBoxImage.Information);
                }
                catch (Exception ex)
                {
                    StatusTextBlock.Text = "Export failed";
                    MessageBox.Show($"Error exporting data: {ex.Message}", "Export Failed", MessageBoxButton.OK, MessageBoxImage.Error);
                }
                finally
                {
                    Mouse.OverrideCursor = null;
                }
            }
        }
        
        protected override void OnClosing(System.ComponentModel.CancelEventArgs e)
        {
            // Clean up database connections when closing the app
            CloseConnection();
            base.OnClosing(e);
        }

        private void ContinueToColumnsButton_FromJoins_Click(object sender, RoutedEventArgs e)
        {
            // Setup the columns tab before navigating to it
            SetupColumnsTab();
            
            // Navigate to the Select Columns tab
            MainTabControl.SelectedIndex = 3;
        }

        #region AI Assistant Implementation

        private void SaveApiKeyButton_Click(object sender, RoutedEventArgs e)
        {
            string apiKey = ApiKeyPasswordBox.Password;
            if (string.IsNullOrWhiteSpace(apiKey))
            {
                MessageBox.Show("Please enter a valid API key.", "Invalid Key", MessageBoxButton.OK, MessageBoxImage.Warning);
                return;
            }

            try
            {
                // Save the API key securely
                SaveApiKey(apiKey);
                
                // Initialize the Gemini service
                geminiService = new GeminiService(apiKey);
                
                MessageBox.Show("API key saved successfully!", "Success", MessageBoxButton.OK, MessageBoxImage.Information);
                
                // Clear the password box for security
                ApiKeyPasswordBox.Clear();
                
                // Add a system message to the chat
                AddSystemMessage("API key saved. I'm ready to help you generate SQL queries!");
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to save API key: {ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void SaveApiKey(string apiKey)
        {
            try
            {
                // This is a simple encryption method - in a production app, consider more secure options
                // like Windows Data Protection API (DPAPI) or a proper secure credential store
                
                // Generate a random salt
                byte[] salt = new byte[16];
                using (var rng = RandomNumberGenerator.Create())
                {
                    rng.GetBytes(salt);
                }
                
                // Create a key derivation function
                using (var pbkdf2 = new Rfc2898DeriveBytes(
                    Environment.MachineName + Environment.UserName, // Simple machine-specific password
                    salt,
                    10000)) // Number of iterations
                {
                    byte[] key = pbkdf2.GetBytes(32); // 256 bits
                    byte[] iv = pbkdf2.GetBytes(16);  // 128 bits
                    
                    // Encrypt the API key
                    byte[] encryptedKey;
                    using (var aes = Aes.Create())
                    {
                        aes.Key = key;
                        aes.IV = iv;
                        
                        using (var encryptor = aes.CreateEncryptor())
                        using (var ms = new MemoryStream())
                        {
                            using (var cs = new CryptoStream(ms, encryptor, CryptoStreamMode.Write))
                            using (var sw = new StreamWriter(cs))
                            {
                                sw.Write(apiKey);
                            }
                            
                            encryptedKey = ms.ToArray();
                        }
                    }
                    
                    // Combine salt and encrypted key for storage
                    byte[] dataToSave = new byte[salt.Length + encryptedKey.Length];
                    Buffer.BlockCopy(salt, 0, dataToSave, 0, salt.Length);
                    Buffer.BlockCopy(encryptedKey, 0, dataToSave, salt.Length, encryptedKey.Length);
                    
                    // Save to file - using the app's local directory instead of AppData which might require elevated permissions
                    try
                    {
                        // First try the original AppData location
                        string appDataFolder = Path.Combine(
                            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                            "SQLDataFetcher");
                            
                        // Create directory if it doesn't exist
                        if (!Directory.Exists(appDataFolder))
                        {
                            Directory.CreateDirectory(appDataFolder);
                        }
                        
                        string filePath = Path.Combine(appDataFolder, apiKeyFileName);
                        File.WriteAllBytes(filePath, dataToSave);
                    }
                    catch (UnauthorizedAccessException)
                    {
                        // If we can't access AppData, fall back to the application's directory
                        string executablePath = AppDomain.CurrentDomain.BaseDirectory;
                        string filePath = Path.Combine(executablePath, apiKeyFileName);
                        
                        // Try writing to the application directory
                        try
                        {
                            File.WriteAllBytes(filePath, dataToSave);
                        }
                        catch (UnauthorizedAccessException)
                        {
                            // If we still can't write, try the user's Documents folder
                            string documentsPath = Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments);
                            string docsFolder = Path.Combine(documentsPath, "SQLDataFetcher");
                            
                            if (!Directory.Exists(docsFolder))
                            {
                                Directory.CreateDirectory(docsFolder);
                            }
                            
                            filePath = Path.Combine(docsFolder, apiKeyFileName);
                            File.WriteAllBytes(filePath, dataToSave);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to save API key securely: {ex.Message}", ex);
            }
        }

        private string LoadApiKey()
        {
            try
            {
                // Try multiple locations in order of preference
                string[] possibleLocations = 
                {
                    // 1. AppData location
                    Path.Combine(
                        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
                        "SQLDataFetcher",
                        apiKeyFileName),
                        
                    // 2. Application directory
                    Path.Combine(AppDomain.CurrentDomain.BaseDirectory, apiKeyFileName),
                    
                    // 3. Documents folder
                    Path.Combine(
                        Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments),
                        "SQLDataFetcher",
                        apiKeyFileName)
                };
                
                // Try each location
                byte[] savedData = null;
                foreach (string filePath in possibleLocations)
                {
                    if (File.Exists(filePath))
                    {
                        try
                        {
                            savedData = File.ReadAllBytes(filePath);
                            break; // Found and loaded the file
                        }
                        catch (UnauthorizedAccessException)
                        {
                            // Try the next location
                            continue;
                        }
                    }
                }
                
                if (savedData == null || savedData.Length < 16)
                {
                    return string.Empty; // No valid data found in any location
                }
                
                // Extract salt (first 16 bytes)
                byte[] salt = new byte[16];
                Buffer.BlockCopy(savedData, 0, salt, 0, 16);
                
                // Extract encrypted data
                byte[] encryptedKey = new byte[savedData.Length - 16];
                Buffer.BlockCopy(savedData, 16, encryptedKey, 0, savedData.Length - 16);
                
                // Derive the same key using the saved salt
                using (var pbkdf2 = new Rfc2898DeriveBytes(
                    Environment.MachineName + Environment.UserName, // Simple machine-specific password
                    salt,
                    10000))
                {
                    byte[] key = pbkdf2.GetBytes(32); // 256 bits
                    byte[] iv = pbkdf2.GetBytes(16);  // 128 bits
                    
                    // Decrypt the API key
                    using (var aes = Aes.Create())
                    {
                        aes.Key = key;
                        aes.IV = iv;
                        
                        using (var decryptor = aes.CreateDecryptor())
                        using (var ms = new MemoryStream(encryptedKey))
                        using (var cs = new CryptoStream(ms, decryptor, CryptoStreamMode.Read))
                        using (var sr = new StreamReader(cs))
                        {
                            return sr.ReadToEnd();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                // Log the exception but return empty string to avoid crashing
                Console.WriteLine($"Error loading API key: {ex.Message}");
                return string.Empty;
            }
        }

        private void UserInputTextBox_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Enter && (Keyboard.Modifiers & ModifierKeys.Shift) != ModifierKeys.Shift)
            {
                e.Handled = true;
                SendQueryButton_Click(sender, e);
            }
        }

        private async void SendQueryButton_Click(object sender, RoutedEventArgs e)
        {
            string userInput = UserInputTextBox.Text.Trim();
            if (string.IsNullOrEmpty(userInput))
            {
                return;
            }

            // Clear the input box
            UserInputTextBox.Text = string.Empty;
            
            // Add the user message to the chat
            AddUserMessage(userInput);
            
            // If not connected to a database, inform the user
            if (activeConn == null || tables.Count == 0)
            {
                AddSystemMessage("Please connect to a database first so I can understand the database schema.");
                return;
            }
            
            // If no API key is set, inform the user
            if (geminiService == null)
            {
                // Try to load the API key
                string apiKey = LoadApiKey();
                if (string.IsNullOrEmpty(apiKey))
                {
                    AddSystemMessage("Please enter your Gemini API key first.");
                    return;
                }
                else
                {
                    geminiService = new GeminiService(apiKey);
                }
            }
            
            try
            {
                // Show loading indicator
                LoadingIndicatorPanel.Visibility = Visibility.Visible;
                
                // Generate database schema description
                string schemaDescription = GeminiService.GenerateDatabaseSchemaDescription(tables);
                
                // Call Gemini API to generate SQL
                string sqlQuery = await geminiService.GenerateSqlFromNaturalLanguageAsync(userInput, schemaDescription);
                
                // Validate the SQL (basic validation)
                bool isValid = GeminiService.ValidateSqlQuery(sqlQuery);
                
                if (!isValid)
                {
                    AddSystemMessage("I'm sorry, but I couldn't generate a safe SQL query from your request. " +
                                     "Please try rephrasing your question, focusing on SELECT operations only.");
                    return;
                }
                
                // Add the AI response with SQL
                AddAiResponseWithSql(sqlQuery);
                
                // Populate the SQL query in the Generate Query tab
                QueryTextBox.Text = sqlQuery;
            }
            catch (Exception ex)
            {
                AddSystemMessage($"Sorry, I encountered an error: {ex.Message}");
            }
            finally
            {
                // Hide loading indicator
                LoadingIndicatorPanel.Visibility = Visibility.Collapsed;
            }
        }

        private void AddUserMessage(string message)
        {
            Border messageBorder = new Border
            {
                BorderBrush = new SolidColorBrush(Color.FromRgb(200, 200, 200)),
                BorderThickness = new Thickness(1),
                Background = new SolidColorBrush(Color.FromRgb(230, 248, 255)),
                CornerRadius = new CornerRadius(5),
                Margin = new Thickness(5),
                Padding = new Thickness(10),
                HorizontalAlignment = HorizontalAlignment.Right,
                MaxWidth = 500
            };
            
            TextBlock messageText = new TextBlock
            {
                Text = message,
                TextWrapping = TextWrapping.Wrap
            };
            
            messageBorder.Child = messageText;
            ChatHistoryPanel.Children.Add(messageBorder);
            
            // Scroll to the bottom
            ChatScrollViewer.ScrollToBottom();
        }

        private void AddSystemMessage(string message)
        {
            Border messageBorder = new Border
            {
                BorderBrush = new SolidColorBrush(Color.FromRgb(208, 208, 208)),
                BorderThickness = new Thickness(1),
                Background = new SolidColorBrush(Color.FromRgb(252, 252, 252)),
                CornerRadius = new CornerRadius(5),
                Margin = new Thickness(5),
                Padding = new Thickness(10),
                HorizontalAlignment = HorizontalAlignment.Left,
                MaxWidth = 500
            };
            
            TextBlock messageText = new TextBlock
            {
                Text = message,
                TextWrapping = TextWrapping.Wrap
            };
            
            messageBorder.Child = messageText;
            ChatHistoryPanel.Children.Add(messageBorder);
            
            // Scroll to the bottom
            ChatScrollViewer.ScrollToBottom();
        }

        private void AddAiResponseWithSql(string sql)
        {
            Border messageBorder = new Border
            {
                BorderBrush = new SolidColorBrush(Color.FromRgb(208, 208, 208)),
                BorderThickness = new Thickness(1),
                Background = new SolidColorBrush(Color.FromRgb(252, 252, 252)),
                CornerRadius = new CornerRadius(5),
                Margin = new Thickness(5),
                Padding = new Thickness(10),
                HorizontalAlignment = HorizontalAlignment.Left,
                MaxWidth = 700
            };
            
            StackPanel panel = new StackPanel();
            
            TextBlock messageText = new TextBlock
            {
                Text = "Here's the SQL query for your request:",
                TextWrapping = TextWrapping.Wrap,
                Margin = new Thickness(0, 0, 0, 10)
            };
            
            Border sqlBorder = new Border
            {
                Background = new SolidColorBrush(Color.FromRgb(240, 240, 240)),
                Padding = new Thickness(10),
                BorderBrush = new SolidColorBrush(Color.FromRgb(200, 200, 200)),
                BorderThickness = new Thickness(1)
            };
            
            TextBlock sqlText = new TextBlock
            {
                Text = sql ?? string.Empty, // Handle possible null SQL
                TextWrapping = TextWrapping.Wrap,
                FontFamily = new FontFamily("Consolas, Courier New, monospace"),
                Foreground = new SolidColorBrush(Color.FromRgb(0, 100, 0))
            };
            
            sqlBorder.Child = sqlText;
            
            Button executeButton = new Button
            {
                Content = "Execute this query",
                Margin = new Thickness(0, 10, 0, 0),
                Padding = new Thickness(10, 5, 10, 5),
                HorizontalAlignment = HorizontalAlignment.Left
            };
            
            executeButton.Click += (sender, e) =>
            {
                // Navigate to SQL query tab
                MainTabControl.SelectedIndex = 5; // Index of "Generate Query" tab
                
                // Set the query text
                QueryTextBox.Text = sql ?? string.Empty; // Handle possible null SQL
                
                // Execute the query
                ExecuteQuery_Click(sender, e);
            };
            
            panel.Children.Add(messageText);
            panel.Children.Add(sqlBorder);
            panel.Children.Add(executeButton);
            
            messageBorder.Child = panel;
            
            if (ChatHistoryPanel != null)
            {
                ChatHistoryPanel.Children.Add(messageBorder);
                
                // Scroll to the bottom
                ChatScrollViewer?.ScrollToBottom();
            }
        }
        
        private void InitializeAiAssistant()
        {
            try
            {
                // Try to load the API key
                string apiKey = LoadApiKey();
                if (!string.IsNullOrEmpty(apiKey))
                {
                    geminiService = new GeminiService(apiKey);
                    AddSystemMessage("API key loaded successfully. I'm ready to help you generate SQL queries!");
                }
            }
            catch (Exception ex)
            {
                // Log exception but don't show a message box on startup
                Console.WriteLine($"Failed to load API key: {ex.Message}");
            }
        }
        
        #endregion
    }
}
