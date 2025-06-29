# Programmatic_Log_File.ingestion

The Programmatic Log File Ingestion Interface script automates the download of daily zipped CSV logs (clicks and impressions), extracts and parses them with encoding fallback, and bulk-uploads each record into an Azure SQL table (AdKernelProgrammaticDataClicks) using ODBC. It also logs every step with rotation and sends notification emails on success or failure.

[HTTP Server: statsfiles.wowcon.net]        [Azure SQL Server]
             ↓                                   ↑
         curl download                          |
             ↓                                   |
   [Local Temp ZIP: F:\Output\file_*.zip]       |
             ↓                                   |
        unzip → F:\Output\file_*                |
             ↓                                   |
  read_csv_with_encoding → encoding detection   |
             ↓                                   |
        csv.reader row by row                   |
             ↓                                   |
       pyodbc.insert rows → AdKernelProgrammaticDataClicks
             ↓
    logging to F:\Logs\clicks_log_YYYY-MM-DD.log
             ↓
     email notifications via SMTP
