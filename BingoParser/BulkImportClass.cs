using RepoDb;
using Serilog;
using System.Timers;
using static BingoParser.ParseArguments;
using Timer = System.Timers.Timer;

namespace BingoParser;

public sealed class BulkImportClass
{
    public static DateTime BulkimportStart { get; set; }

    public static void BulkImport() {
        BulkimportStart = DateTime.Now;
        var theTimer = new Timer {
            Interval = 1000,
            Enabled = true,
            AutoReset = true
        };
        theTimer.Elapsed += OnTimerElapsed;

        try {
            theTimer.Start();
            ExecuteBulkImport();
        }
        catch (Exception e) {
            Console.WriteLine($"{e}\n{e.Message}");
            Log.Error($"{e}\n{e.Message}");
            throw;
        }
        finally {
            theTimer.Stop();
        }

        if (!Quiet) Console.WriteLine();
    }

    private static void OnTimerElapsed(object? sender, ElapsedEventArgs e) {
        var elapsed = e.SignalTime - BulkimportStart;
        if (!Quiet) Console.Write($"\rPreimportazione in corso... {elapsed.Minutes:00}:{elapsed.Seconds:00}");
    }

    public static void ExecuteBulkImport() {
        Log.Information("Inizio preimportazione...");
#if DEBUG
        const string fileTsv = "/var/opt/mssql/data/data/AllReadings.tsv";
#else
        const string fileTsv = @"c:\Data\AllReadings.tsv";
#endif
        using var db = Connector.Create();
        if (NewImport) {
            Log.Information($"La tabella {BulkImportTableName} è stata svuotata (flag -w)");
            const string sql = "truncate table Misure.Preimport";
            var _ = db.ExecuteNonQuery(sql);
            // ReSharper disable once LocalizableElement
            if (!Quiet) Console.WriteLine($"La tabella {BulkImportTableName} è stata svuotata.");
        }
        var commandText = $@"bulk insert Misure.Preimport
                                 from '{fileTsv}'
                                 with (
                                    FIELDTERMINATOR = '\t',
                                    ROWTERMINATOR = '\n',
                                    FIRSTROW = 2
                                )";
        var affectedRows = db.ExecuteNonQuery(commandText, commandTimeout: 0);
        // ReSharper disable once LocalizableElement
        if (!Quiet) Console.WriteLine($"\nPreimportazione versione 6 terminata - inserite {affectedRows} righe.");
        Log.Information($"Preimportazione versione 6 terminata - inserite {affectedRows} righe.");
    }
}