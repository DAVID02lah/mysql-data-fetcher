using System;
using System.Collections.Generic;
using System.Windows.Controls;

namespace SQLDataFetcher.Models
{
    /// <summary>
    /// Represents an ORDER BY entry in the SQL query
    /// </summary>
    public class OrderByEntry
    {
        public ComboBox? ColumnComboBox { get; set; }
        public ComboBox? OrderComboBox { get; set; }
    }

    /// <summary>
    /// Represents a GROUP BY entry in the SQL query
    /// </summary>
    public class GroupByEntry
    {
        public ComboBox? ColumnComboBox { get; set; }
    }

    /// <summary>
    /// Represents an aggregate function entry in the SQL query
    /// </summary>
    public class AggregateEntry
    {
        public ComboBox? FunctionComboBox { get; set; }
        public ComboBox? ColumnComboBox { get; set; }
        public TextBox? AliasTextBox { get; set; }
    }

    /// <summary>
    /// Represents a WHERE condition entry in the SQL query
    /// </summary>
    public class WhereEntry
    {
        public ComboBox? ColumnComboBox { get; set; }
        public ComboBox? OperatorComboBox { get; set; }
        public TextBox? ValueTextBox { get; set; }
        public ComboBox? ConnectorComboBox { get; set; }
    }

    /// <summary>
    /// Represents a combined column using COALESCE in the SQL query
    /// </summary>
    public class CombinedColumnEntry
    {
        public TextBox? AliasTextBox { get; set; }
        public Dictionary<string, CheckBox>? ColumnCheckboxes { get; set; }
        public StackPanel? CheckboxesPanel { get; set; }
    }

    /// <summary>
    /// Represents a JOIN configuration in the SQL query
    /// </summary>
    public class JoinEntry
    {
        public ComboBox? LeftTableComboBox { get; set; } // Nullable since it's optional for subsequent joins
        public ComboBox? RightTableComboBox { get; set; }
        public ComboBox? JoinTypeComboBox { get; set; }
        public ComboBox? LeftColumnComboBox { get; set; }
        public ComboBox? RightColumnComboBox { get; set; }
    }
}
