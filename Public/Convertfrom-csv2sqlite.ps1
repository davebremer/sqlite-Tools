Function Convertfrom-csv2sqlite {
<#
.SYNOPSIS
 Convert a CSV file into an sqlite database

.DESCRIPTION
 Converts a CSV file into an sqlite database. If the database file does not exist then it is created.

 If the database is created then the columns of the table are created from the first line of the 
 CSV file. Any double quote marks are stripped. All fields are type TEXT
 
 If the database file exists then the data is appended to an existing table. An error is thrown if the 
 tablename in the database  doesn't match the table name used in the query. You can supply the 
 table name in a parameter, otherwise the file name is used as the table name (the default table
 name does not include the file suffix)

 An error is also thrown if the existing table does not have fields matching the CSV file headings


.PARAMETER CsvFile
 The name of the CSV file to be converted. The first line must contain headings.

.PARAMETER Database
 The name of the SQLite database file

.PARAMETER TableName
 The name of the table to import the CSV file. If not provided, then the name of the file is used
 as the table name - excluding the suffix. The table must contain fields matching the CSV header line.


.PARAMETER BufferSize
 The number of records to be imported in a batch. Defaults to 1000.

.EXAMPLE
 Convertfrom-csv2sqlite  -CsvFile "example.csv" -Database "wibble.sqlite"

 If the file wibble.sqlite does not exist then it is created with a single table named 'example'.
 This will have fields based on the csv file heading.

 If the file exists then the data is appended to the 'example' table. If the table does not exist
 then an "ExecuteNonQuery" exception is thrown

.EXAMPLE
  Convertfrom-csv2sqlite  -CsvFile "example.csv" -Database "wibble.sqlite" -TableName "foo"

  The table 'foo' is used rather than 'example'. 

.INPUTS
 CSV file

.OUTPUTS
 SQLite file
 
.LINK
 https://github.com/RamblingCookieMonster/PSSQLite

.NOTES
 Author: Dave Bremer
 Date: 2018-11-12

 Updates:

 TODO
   1) Try/Catch around database actions (create, insert)
   2) Add a summary of the table to the verbose stream
   3) Add whatif
#>

#Requires –Modules pssqlite
[CmdletBinding()] 
param(
    [Parameter(
        Mandatory = $TRUE,
        Position = 1,
        HelpMessage = 'CSV File'
    )]
    [String]$CsvFile,

    [Parameter(
        Mandatory = $TRUE,
        Position = 2,
        HelpMessage = 'Database File'
    )]
    [String]$Database,

    [Parameter( 
        Mandatory = $FALSE,
        HelpMessage = 'Table name'
    )]
    [String]$TableName = ((Split-path $CsvFile -Leaf).Split(".")[0]),

    [Parameter(
        Mandatory = $FALSE,
        HelpMessage = 'Buffer Size'
    )]
    [ValidateRange(1, [int]::MaxValue)]
    [int64]$BufferSize=1000
  ) 

BEGIN{
    
    $dbExists = (Test-Path $Database -PathType Leaf)
    Write-Verbose ("Database file: `"{0}`" - Exists? {1}" -f $Database,$dbExists)
    Write-Verbose ("CSV file: `"{0}`"" -f $CsvFile)
    Write-Verbose ("Table: `"{0}`"" -f $Tablename)
    Write-Verbose ("Buffer Size: {0}" -f $BufferSize)

    $values = @()
    $datatable = $null
    $conn = New-SQLiteConnection -DataSource $Database

    $recordcount = 0
}

PROCESS{
    if (! $dbExists ) { #create database
        # OK this is a kludge. Stripping quotes which sometimes surround text in a CSV.
        # I don't like this though. What if there's a quote in the text?
        $headings = (Get-Content $CsvFile -first 1).Replace("`"","")
        $createQuery = ("CREATE TABLE [{0}] ([{1}] TEXT)" -f $TableName,
            ($headings.Replace(",","] TEXT,[")))
        Write-Verbose ("Create Query: {0}" -f $createQuery)
        
        try {
            Invoke-SqliteQuery -SQLiteConnection $conn -Query $createquery
        } catch {
            $conn.Close()
            throw $_
        }
    }

    # Stream an import-csv rather than trying to read the whole file into a variable
    # Allows massive files to be imported with consistant memory impact
    Import-Csv $CsvFile |
        ForEach-Object {
            $values += $_
            $recordcount++
        
            if ($values.Count -ge $BufferSize -1) {
                $datatable = ($values | Out-DataTable)

                try {          
                    Invoke-SQLiteBulkCopy -SQLiteConnection $conn -DataTable $datatable -Table $tablename -Force 
                } Catch {
                    $conn.Close()
                    throw $_
                }
                $values = @()
            }
    
        } # Foreach-Object  

        #Write out any left in buffer
        if ($values.Count -gt 0) {
            $datatable = $values | Out-DataTable

            try {          
                Invoke-SQLiteBulkCopy -SQLiteConnection $conn -DataTable $datatable -Table $tablename -Force 
            } Catch {
                $conn.Close()
                throw $_
            }
            $values = @()
        }
        
        Write-Verbose ("{0} records added to table `"{1}`" in database file `"{2}`"" -f $recordcount,$TableName, $Database)
        $query = ("SELECT count(*) as total from [{0}]" -f $TableName)
        Write-Verbose ("Total records now {0}" -f (Invoke-SqliteQuery -SQLiteConnection $conn -query $query).total)
}
END{
    $conn.Close()}
}