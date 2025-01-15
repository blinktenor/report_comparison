require 'duckdb'
require 'csv'

folder = 'sjcdsbs'.freeze

FIRST_HEADER = 'Ending Balance'.freeze
old_csv = folder + '/old.csv'
new_csv = folder + '/new.csv'
db_file_path = folder + '/' + folder + '.duckdb'

def csv_to_duckdb(csv_file_path, db_file_path, table_name)
  puts "Converting #{csv_file_path} to DuckDB table #{table_name} in #{db_file_path}..."

  # Connect to DuckDB
  conn = DuckDB::Database.open(db_file_path).connect

  # Create table schema based on CSV headers
  csv = CSV.open(csv_file_path)
  line = csv.readline

  while (!line.map { |header| "#{header}" }.join(", ").include?(FIRST_HEADER))
    line = csv.readline
  end
  headers = strip_spaces(line)
  columns = headers.map { |header| "\"#{header}\" TEXT" }.join(",")

  # Drop the table if it exists
  drop_table_sql = "DROP TABLE IF EXISTS #{table_name}"
  conn.execute(drop_table_sql)

  create_table_sql = "CREATE TABLE IF NOT EXISTS #{table_name} (label TEXT, #{columns}, index INT);"
  conn.execute(create_table_sql)

  labels = []

  # Insert CSV data into DuckDB table
  csv.each_with_index do |row, index|
    label = row.first
    labels << label
    values = row.map { |field| "'#{field != nil ? field.gsub("'", "''") : '0' }'" }.join(",") # Exclude the label
    insert_sql = "INSERT INTO #{table_name} VALUES (#{values}, #{index});"
    conn.execute(insert_sql)
  end

  # Create a table to store labels
  conn.execute("CREATE TABLE IF NOT EXISTS Labels (label TEXT, label_index INT);")

  labels = labels.compact.uniq

  # Insert labels into the Labels table
  labels.each do |label, index|
    # Check if the label already exists
    result = conn.execute("SELECT 1 FROM Labels WHERE label = '#{label.gsub("'", "''")}' LIMIT 1;")
    if result.count == 0
      # Insert the label if it doesn't exist
      conn.execute("INSERT INTO Labels (label) VALUES ('#{label.gsub("'", "''")}', #{index});")
    end
  end

  # Create a table to store headers and their index
  conn.execute("CREATE TABLE IF NOT EXISTS Headers (header TEXT, header_index INT);")

  # Insert headers and their index into the Headers table
  headers.each_with_index do |header, index|
    # Check if the header already exists
    result = conn.execute("SELECT 1 FROM Headers WHERE header = '#{header.gsub("'", "''")}' LIMIT 1;")
    if result.count == 0
      # Insert the header if it doesn't exist
      conn.execute("INSERT INTO Headers (header, header_index) VALUES ('#{header.gsub("'", "''")}', #{index});")
    end
  end

  # Close the connection
  conn.close
end

def compare_csvs_to_duckdb(old_csv, new_csv, db_file_path)
  # Convert CSVs to DuckDB tables
  csv_to_duckdb(old_csv, db_file_path, 'Original_Records')
  csv_to_duckdb(new_csv, db_file_path, 'New_Records')

  # Connect to DuckDB
  conn = DuckDB::Database.open(db_file_path).connect

  # Get headers from the Headers table
  headers = conn.execute("SELECT header FROM Headers").map { |row| row[0] }

  # Drop the Differences table if it exists
  conn.execute("DROP TABLE IF EXISTS Differences")

  puts "Comparing Original_Records to New_Records..."
  # Create the Differences table
  create_differences_table_sql = "CREATE TABLE Differences (source TEXT, label TEXT, #{headers.map { |header| "\"#{header}\" TEXT" }.join(", ")}, index INT)"
  puts create_differences_table_sql
  conn.execute(create_differences_table_sql)

  # Build the SQL query to find non-matching rows
  comparison_sql = <<-SQL
    INSERT INTO Differences
    SELECT 'Original_Records' AS source, o.*
    FROM Original_Records o
    LEFT JOIN New_Records n
    ON #{headers.map { |header| "o.\"#{header}\" = n.\"#{header}\"" }.join(" AND ")}
    WHERE #{headers.map { |header| "o.\"#{header}\" IS DISTINCT FROM n.\"#{header}\"" }.join(" OR ")}
    UNION
    SELECT 'New_Records' AS source, n.*
    FROM New_Records n
    LEFT JOIN Original_Records o
    ON #{headers.map { |header| "n.\"#{header}\" = o.\"#{header}\"" }.join(" AND ")}
    WHERE #{headers.map { |header| "n.\"#{header}\" IS DISTINCT FROM o.\"#{header}\"" }.join(" OR ")}
  SQL

  # Execute the comparison SQL
  conn.execute(comparison_sql)

  puts "closing connection"
  # Close the connection
  conn.close
end

def strip_spaces(line = [])
  line.map { |header| header != nil ? header.gsub(/\n/, ' ') : ''}.reject { |header| header == '' }
end

compare_csvs_to_duckdb(old_csv, new_csv, db_file_path)