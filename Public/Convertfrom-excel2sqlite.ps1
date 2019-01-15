Function Convertfrom-Excel2SQLite {
<#
.SYNOPSIS
 Convert an Excel file into an sqlite database

.DESCRIPTION

#TODO
 


.PARAMETER ExcelFile
 The name of the Excel file to be converted. The first line must contain headings.

.PARAMETER SheetName
 The name of the spreadsheet within the Excel file to import

.PARAMETER Database
 The name of the SQLite database file

.PARAMETER TableName
 The name of the table to import into. If not provided, then the name of the Sheet is used
 as the table name - excluding the suffix. The table must contain fields matching the sheet's header line.

.EXAMPLE

.INPUTS
 Excel file

.OUTPUTS
 SQLite file
 
.LINK
 https://github.com/RamblingCookieMonster/PSSQLite

.NOTES
 Author: Dave Bremer
 Date: 2018-12-04

 Updates:
 
 TODO:
    1) Allow an array of sheetnames to add multiple
    2) Add a -ALLSHEETS which adds all sheets to the database using the sheet name as the tablename 
#>

#Requires –Modules pssqlite
[CmdletBinding()] 
param(
    [Parameter(
        Mandatory = $TRUE,
        Position = 1,
        HelpMessage = 'Excel File'
    )]
    [String]$ExcelFile,

    [Parameter(
        Mandatory = $TRUE,
        Position = 1,
        HelpMessage = 'Sheet name'
    )]
    [String]$SheetName,

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
    [String]$TableName = $SheetName
  ) 

BEGIN{

    $dbExists = (Test-Path $Database -PathType Leaf)    
    Write-Verbose ("Database file: `"{0}`" - Exists? {1}" -f $Database,$dbExists)
    Write-Verbose ("Excel file: `"{0}`"" -f $ExcelFile)
    Write-Verbose ("Sheet Name: `"{0}`"" -f $SheetName)
    Write-Verbose ("Table: `"{0}`"" -f $Tablename)
   
    Write-Verbose "create object" 
    $Excel = New-Object -ComObject Excel.Application
    
    write-verbose ("open workbook `"{0}`"" -f $ExcelFile)
    $Workbook = $Excel.Workbooks.Open($excelfile)

    
    $conn = New-SQLiteConnection -DataSource $Database

    
}

PROCESS{

<# TODO - if the worksheet isn't specified then loop through all worksheets
    Make an array of worksheets either taking the parameter as that, or make an array of all worksheets
    Loop through each sheet adding to a table by that name
    
    #>


    Write-Verbose ("Opening sheet `"{0}`"" -f $SheetName)
    $theSheet = $workbook.worksheets.item($SheetName)    

    $maxcol = ($theSheet.UsedRange.Columns).count
    $maxrow = ($theSheet.UsedRange.rows).count
    Write-Verbose ("Columns: {0}" -f $maxcol)
    Write-Verbose ("Rows: {0}" -f ($maxrow-1)) #minus the header row
    
    $values = @()
    $datatable = $null

    # See if the table exists in the database. If its not there it'll return null
    $query = ("SELECT * FROM sqlite_master where tbl_name LIKE `'{0}`'" -f $TableName)
    $table = Invoke-SqliteQuery -SQLiteConnection $conn -Query $query

    $recordcount = 0

    # get the header values
    $header = @() #an array of the header values
    for ($col = 1; $col -le $maxcol; $col++) {
        $header += ,$theSheet.cells.item(1,$col).value2
    }
    Write-Verbose ("Header: {0}" -f ($header -join '|'))

    if ( ! $dbExists -or ! $table ) { #create database

        
        $createQuery = ("CREATE TABLE IF NOT EXISTS [{0}] (" -f $TableName)
        for ($col = 1; $col -le $maxcol; $col++) {
            if ($col -gt 1) {$createQuery += ","}
            $createQuery += $theSheet.cells.item(1,$col).value2
            $createQuery += " TEXT"
        }

        $createQuery += ")"
        Write-Verbose ("Create Query: {0}" -f $createQuery)
        
        try {
            Invoke-SqliteQuery -SQLiteConnection $conn -Query $createquery
        } catch {
            $conn.Close()
            throw $_
        }
    }

    
    for ($rownum = 2; $rownum -le $maxrow; $rownum++) {
        $roW = @{}
       
        for ($col=0;$col -lt $header.count;$col++) {
            $row.add($header[$col].ToString(),$theSheet.cells.item($rownum,$col+1).value2.tostring())    
        }
        $Values += (New-Object -TypeName PSObject -Property $row)
    
    }

    $datatable = ($Values | Out-DataTable)
    
    try {          
        Invoke-SQLiteBulkCopy -SQLiteConnection $conn -DataTable $datatable -Table $tablename -Force 
    } Catch {
        $conn.Close()
        throw $_
    }
<#
        
    Write-Verbose ("{0} records added to table `"{1}`" in database file `"{2}`"" -f $maxrow -1,$TableName, $Database)
    $query = ("SELECT count(*) as total from [{0}]" -f $TableName)
    Write-Verbose ("Total records now {0}" -f (Invoke-SqliteQuery -SQLiteConnection $conn -query $query).total)
#>
}
END{
    $conn.Close()}
}