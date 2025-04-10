using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Json; // Ensure you have the System.Net.Http.Json NuGet package installed
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace SQLDataFetcher.Services
{
    public class GeminiService
    {
        private readonly HttpClient _httpClient;
        private readonly string _apiKey;
        // Updated to use the v1 API with gemini-1.5-flash model
        private readonly string _apiUrl = "https://generativelanguage.googleapis.com/v1/models/gemini-1.5-flash:generateContent";
        
        public GeminiService(string apiKey)
        {
            if (string.IsNullOrWhiteSpace(apiKey))
            {
                throw new ArgumentNullException(nameof(apiKey), "API key cannot be null or empty.");
            }
            _apiKey = apiKey;
            _httpClient = new HttpClient();
            // Optional: Set a default timeout
            _httpClient.Timeout = TimeSpan.FromSeconds(60);
        }

        public async Task<string> GenerateSqlFromNaturalLanguageAsync(string userPrompt, string databaseSchema)
        {
            if (string.IsNullOrWhiteSpace(userPrompt))
            {
                throw new ArgumentNullException(nameof(userPrompt), "User prompt cannot be null or empty.");
            }
            if (string.IsNullOrWhiteSpace(databaseSchema))
            {
                throw new ArgumentNullException(nameof(databaseSchema), "Database schema cannot be null or empty.");
            }

            try
            {
                // Create the system prompt with schema information
                string systemPrompt = $@"You are an expert SQL query generator.
Given the following database schema and a user request, generate a valid SQL query.
Only output the SQL query, nothing else. Do not include any explanations, comments, or markdown formatting (like ```sql ... ```).
Make sure the query is syntactically correct and follows best practices for standard SQL.

Your task is to create SQL SELECT queries. The user may request JOINs between tables - handle these appropriately
to avoid duplicate data. Always use table aliases for clarity and proper column references.

These specific requirements must be followed:
1. ONLY generate SELECT queries
2. Use proper JOIN syntax with ON conditions when joining tables
3. Use table aliases for all tables (like t1, t2 or more descriptive names)
4. Include column name prefixes with table aliases for clarity (like t1.column_name)
5. When the user requests a column from a specific table, ensure it comes from that table
6. When joining tables, ensure the JOIN conditions are appropriate to avoid duplicate data
7. Never exclude requested columns, even if complex joins are required
8. Do not use the function 'strftime' or 'STRFTIME' in the SQL query

DATABASE SCHEMA:
{databaseSchema}

USER REQUEST: {userPrompt}

Return ONLY the SQL query without ANY additional explanation or formatting.";

                // Create request payload (structure is generally the same for v1 API)
                var payload = new
                {
                    contents = new[]
                    {
                        new
                        {
                            // Using "user" role is standard practice for the generateContent endpoint
                            role = "user",
                            parts = new[]
                            {
                                new
                                {
                                    text = systemPrompt
                                }
                            }
                        }
                    },
                    
                    generationConfig = new
                    {
                        temperature = 1.0, // Adjust as needed for creativity vs. accuracy
                    }
                };

                // Serialize and send the request
                string requestUri = $"{_apiUrl}?key={_apiKey}";
                HttpResponseMessage? response = null; // Declare outside try block for potential logging in catch

                try
                {
                    response = await _httpClient.PostAsJsonAsync(requestUri, payload);

                    if (!response.IsSuccessStatusCode)
                    {
                        string errorContent = await response.Content.ReadAsStringAsync();
                        // Try to parse Google API error for more details
                        string detailedError = TryParseGoogleApiError(errorContent);
                        throw new HttpRequestException($"API call failed with status {response.StatusCode}. Response: {(string.IsNullOrEmpty(detailedError) ? errorContent : detailedError)}");
                    }

                    // Parse the response
                    string jsonResponse = await response.Content.ReadAsStringAsync();
                    using (JsonDocument doc = JsonDocument.Parse(jsonResponse))
                    {
                        JsonElement root = doc.RootElement;

                        // Standard path to the generated text
                        if (root.TryGetProperty("candidates", out JsonElement candidates) &&
                            candidates.ValueKind == JsonValueKind.Array &&
                            candidates.GetArrayLength() > 0 &&
                            candidates[0].TryGetProperty("content", out JsonElement content) &&
                            content.TryGetProperty("parts", out JsonElement parts) &&
                            parts.ValueKind == JsonValueKind.Array &&
                            parts.GetArrayLength() > 0 &&
                            parts[0].TryGetProperty("text", out JsonElement text))
                        {
                            // Trim potential markdown backticks and whitespace
                            string rawSql = text.GetString() ?? string.Empty;
                            return rawSql.Trim().Trim('`').Trim(); // Basic cleaning
                        }

                        // Handle potential safety blocks or other response structures
                        if (root.TryGetProperty("promptFeedback", out JsonElement promptFeedback) &&
                            promptFeedback.TryGetProperty("blockReason", out JsonElement blockReason))
                        {
                            throw new Exception($"API request was blocked. Reason: {blockReason.GetString()}. Check safety settings or prompt content.");
                        }

                        throw new Exception($"Could not parse SQL query from API response. Response JSON: {jsonResponse}");
                    }
                }
                catch (HttpRequestException httpEx)
                {
                    // Re-throw HTTP exceptions with potentially more context
                    throw new Exception($"Error during API call: {httpEx.Message}", httpEx);
                }
                catch (JsonException jsonEx)
                {
                    string rawContent = response != null ? await response.Content.ReadAsStringAsync() : "Response content unavailable";
                    throw new Exception($"Error parsing JSON response: {jsonEx.Message}. Raw response: {rawContent}", jsonEx);
                }
                catch (TaskCanceledException timeEx) when (timeEx.InnerException is TimeoutException)
                {
                     throw new Exception($"API request timed out after {_httpClient.Timeout.TotalSeconds} seconds.", timeEx);
                }
                catch (TaskCanceledException cancelEx)
                {
                    // Handle general cancellation (less common unless explicitly cancelled)
                    throw new Exception($"API request was canceled.", cancelEx);
                }
            }
            catch (Exception ex) // Catch exceptions from prompt generation or argument validation
            {
                // Wrap the exception to provide context
                throw new Exception($"Error generating SQL: {ex.Message}", ex);
            }
        }

        /// <summary>
        /// Attempts to parse a more specific error message from Google API error JSON.
        /// </summary>
        private string TryParseGoogleApiError(string errorContent)
        {
            try
            {
                using (JsonDocument doc = JsonDocument.Parse(errorContent))
                {
                    if (doc.RootElement.TryGetProperty("error", out JsonElement errorElement) &&
                        errorElement.TryGetProperty("message", out JsonElement messageElement))
                    {
                        return messageElement.GetString() ?? errorContent;
                    }
                }
            }
            catch (JsonException)
            {
                // Ignore if parsing fails, return original content
            }
            return errorContent; // Return original content if parsing fails or structure is different
        }


        /// <summary>
        /// Extracts the database schema information in a format suitable for the LLM.
        /// </summary>
        public static string GenerateDatabaseSchemaDescription(
            Dictionary<string, List<string>> tables)
        {
            if (tables == null || tables.Count == 0)
            {
                return "No database schema provided.";
            }

            var schemaBuilder = new StringBuilder();
            schemaBuilder.AppendLine("TABLES AND COLUMNS:"); // Clearer heading

            foreach (var table in tables)
            {
                if (string.IsNullOrWhiteSpace(table.Key) || table.Value == null) continue; // Skip invalid entries

                // Use standard SQL commenting style for clarity within the prompt
                schemaBuilder.AppendLine($"-- Table: {table.Key}");
                // schemaBuilder.AppendLine("Columns:"); // Less necessary with comment style

                if (table.Value.Count > 0)
                {
                    foreach (var column in table.Value)
                    {
                        if (!string.IsNullOrWhiteSpace(column))
                        {
                             // Assuming column might contain type info, e.g., "UserID INT PRIMARY KEY"
                             // If just names, `  -- {column}` is fine.
                            schemaBuilder.AppendLine($"  -- {column.Trim()}");
                        }
                    }
                }
                else
                {
                    schemaBuilder.AppendLine("  -- (No columns listed for this table)");
                }
                schemaBuilder.AppendLine(); // Add space between tables
            }

            return schemaBuilder.ToString();
        }

        /// <summary>
        /// Validates that a SQL query appears safe to run (basic check).
        /// IMPORTANT: This is NOT a foolproof security measure against SQL injection.
        /// Use parameterized queries when executing SQL. This check is only a preliminary filter
        /// against accidental generation of destructive commands by the LLM.
        /// </summary>
        public static bool ValidateSqlQuery(string sqlQuery)
        {
            if (string.IsNullOrWhiteSpace(sqlQuery))
                return false;

            // Convert to lowercase for case-insensitive checking
            string lowerQuery = sqlQuery.Trim().ToLowerInvariant();

            // 1. Ensure it starts with SELECT (allowing for leading comments potentially)
            // Find the first non-comment word
            string firstWord = GetFirstSqlKeyword(lowerQuery);
            if (firstWord != "select")
            {
                Console.Error.WriteLine($"Validation failed: Query does not start with SELECT (first keyword: '{firstWord}'). Query: {sqlQuery}");
                return false;
            }

            // 2. Check for dangerous keywords/commands that modify data
            string[] dangerousKeywords = {
                "drop", "truncate", "delete", "alter", "update", "insert",
                "create", "exec", "execute", "sp_", "xp_"
            };

            // Improved check - more careful about word boundaries
            foreach (var keyword in dangerousKeywords)
            {
                // Skip checking keywords that might be part of column names in this specific query
                if (keyword == "update" && sqlQuery.Contains("update_")) continue;
                if (keyword == "create" && sqlQuery.Contains("create_")) continue;
                
                // Use a more sophisticated check to avoid false positives
                if (ContainsKeywordAsCommand(lowerQuery, keyword))
                {
                    Console.Error.WriteLine($"Validation failed: Query contains potentially dangerous keyword '{keyword}'. Query: {sqlQuery}");
                    return false;
                }
            }

            // 3. Check for multiple statements (though the prompt asks for one)
            if (CountStatements(lowerQuery) > 1)
            {
                Console.Error.WriteLine($"Validation failed: Query appears to contain multiple statements. Query: {sqlQuery}");
                return false;
            }

            return true; // Passed basic checks
        }

        // Helper to check if a keyword appears as an actual SQL command (not part of another word)
        private static bool ContainsKeywordAsCommand(string query, string keyword)
        {
            int index = 0;
            while ((index = query.IndexOf(keyword, index, StringComparison.OrdinalIgnoreCase)) >= 0)
            {
                bool validPrefix = index == 0 || !char.IsLetterOrDigit(query[index - 1]);
                bool validSuffix = index + keyword.Length >= query.Length || 
                                   !char.IsLetterOrDigit(query[index + keyword.Length]);
                
                if (validPrefix && validSuffix)
                {
                    // Check if it's in a comment or string
                    if (!IsInsideCommentOrString(query, index))
                        return true;
                }
                index += keyword.Length;
            }
            return false;
        }

        // Basic check if position is inside a comment or string
        private static bool IsInsideCommentOrString(string query, int position)
        {
            // This is a simplified check - a full SQL parser would be more accurate
            bool inSingleQuote = false;
            bool inDoubleQuote = false;
            bool inLineComment = false;
            bool inBlockComment = false;
            
            for (int i = 0; i < position; i++)
            {
                if (inLineComment)
                {
                    if (query[i] == '\n')
                        inLineComment = false;
                    continue;
                }
                
                if (inBlockComment)
                {
                    if (i > 0 && query[i-1] == '*' && query[i] == '/')
                        inBlockComment = false;
                    continue;
                }
                
                if (!inSingleQuote && !inDoubleQuote)
                {
                    if (i < query.Length - 1)
                    {
                        if (query[i] == '-' && query[i + 1] == '-')
                        {
                            inLineComment = true;
                            continue;
                        }
                        
                        if (query[i] == '/' && query[i + 1] == '*')
                        {
                            inBlockComment = true;
                            continue;
                        }
                    }
                }
                
                if (!inDoubleQuote && query[i] == '\'')
                    inSingleQuote = !inSingleQuote;
                
                if (!inSingleQuote && query[i] == '"')
                    inDoubleQuote = !inDoubleQuote;
            }
            
            return inSingleQuote || inDoubleQuote || inLineComment || inBlockComment;
        }

        // Count actual SQL statements (separated by semicolons, excluding those in strings/comments)
        private static int CountStatements(string query)
        {
            int count = 0;
            bool inSingleQuote = false;
            bool inDoubleQuote = false;
            bool inLineComment = false;
            bool inBlockComment = false;
            
            for (int i = 0; i < query.Length; i++)
            {
                if (inLineComment)
                {
                    if (query[i] == '\n')
                        inLineComment = false;
                    continue;
                }
                
                if (inBlockComment)
                {
                    if (i > 0 && query[i-1] == '*' && query[i] == '/')
                        inBlockComment = false;
                    continue;
                }
                
                if (!inSingleQuote && !inDoubleQuote && !inLineComment && !inBlockComment)
                {
                    if (query[i] == ';')
                        count++;
                        
                    if (i < query.Length - 1)
                    {
                        if (query[i] == '-' && query[i + 1] == '-')
                            inLineComment = true;
                        
                        if (query[i] == '/' && query[i + 1] == '*')
                            inBlockComment = true;
                    }
                }
                
                if (!inDoubleQuote && query[i] == '\'')
                    inSingleQuote = !inSingleQuote;
                
                if (!inSingleQuote && query[i] == '"')
                    inDoubleQuote = !inDoubleQuote;
            }
            
            // If no semicolons found, assume at least one statement
            return Math.Max(1, count);
        }

        // Helper to find the first SQL keyword, skipping potential comments
        private static string GetFirstSqlKeyword(string query)
        {
            query = query.Trim();
            if (query.StartsWith("--")) // Skip single-line comment
            {
                int nextLine = query.IndexOf('\n');
                if (nextLine == -1) return string.Empty; // Only comment
                return GetFirstSqlKeyword(query.Substring(nextLine + 1));
            }
            if (query.StartsWith("/*")) // Skip multi-line comment
            {
                int endComment = query.IndexOf("*/");
                 if (endComment == -1) return string.Empty; // Unterminated comment
                 return GetFirstSqlKeyword(query.Substring(endComment + 2));
            }

            // Return the first word
            string[] parts = query.Split(new[] { ' ', '\t', '\r', '\n', '(' }, StringSplitOptions.RemoveEmptyEntries);
            return parts.Length > 0 ? parts[0] : string.Empty;
        }

    }
}