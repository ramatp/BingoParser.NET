using RepoDb;
using Serilog;
using System.Data;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using static BingoParser.BulkImportClass;
using static BingoParser.ParseArguments;
using PmMapper = BingoParser.Model.PmMapper;

namespace BingoParser;

public static class Program
{
    private static long RowsWritten = 0;
    private static long TotalRows = 0;
    public static StreamWriter? OutputStream { get; set; }
    public static ICollection<PmMapper>? PmMap { get; set; } // Contiene il join tra SourceTable, SourceFilter e codice PdM
    public static string RowExpectedFormat { get; } = @"^\w+\s*[,;]?\s*(19|20)\d\d[- /.]?(0[1-9]|1[012])[- /.]?(0[1-9]|[12][0-9]|3[01])[T ]?([01][1-9]|2[0123]):?([0-5][0-9]):?([0-5][0-9])?\s*[,;]?\s*[-+]?[0-9]*[,\.]?[0-9]+\s*[,;]?\s*[-+]?[0-9]*[,\.]?[0-9]+.*?$";

    public static CultureInfo LocCulture { get; } = CultureInfo.CreateSpecificCulture("en-US");
    public static string[] HeaderData { get; } = {
        "PmId",
        "RDateTime",
        "RText",
        "RValue",
        "RTaken"
    };

    public static void Main(string[] args) {
        SqlServerBootstrap.Initialize();
        LocCulture.DateTimeFormat.TimeSeparator = ":";
        LocCulture.DateTimeFormat.DateSeparator = "-";
        LocCulture.NumberFormat.NumberDecimalSeparator = ".";
        LocCulture.NumberFormat.CurrencyDecimalSeparator = ".";

        // Configurazione Serilog
        Log.Logger = new LoggerConfiguration()
                     .MinimumLevel.Information()
                     .Enrich.WithEnvironmentUserName()
                     .WriteTo.File(@"Logs\Log.txt", rollingInterval: RollingInterval.Day,
                                   outputTemplate: "[{Timestamp:HH:mm:ss}] [{Level:u3}] {EnvironmentUserName}  {Message:lj}{NewLine}{Exception}")
                     .CreateLogger();

        try {
            Splash();
            if (Help) return;
            Parse(args);
            if (!Directory.Exists(InputDirectory)) {
                if (!Quiet) Console.WriteLine($"La directory {InputDirectory} non esiste.");
                return;
            }

            if (!Directory.Exists(OutputDirectory)) Directory.CreateDirectory(OutputDirectory!);

            using var db = Connector.Create();
            PmMap = db.ExecuteQuery<PmMapper>(@"select pdmId, SourceTable, SourceFilter from T30_Import where isAutoImported = 1").ToList();
            if (!BulkImportOnly) ConvertAllInputFiles();

            if (!ConvertOnly) BulkImport();
#if DEBUG
            if (!Quiet) Console.Write("Preimportazione terminata. Premere un tasto per uscire.");
            Console.ReadKey();
#endif

        }
        catch (Exception ex) {
            Log.Error($"{ex}\n{ex.Message}");
            Console.WriteLine($"Exception {ex} occurred.\nDetailed explanation:\n{ex.Message}");
        }
        finally {
            Log.CloseAndFlush();
        }
    }

    public static string? GetPm(string table, string filter) {
        try {
            var pm = PmMap!.SingleOrDefault(x => x.SourceTable == table && x.SourceFilter == filter);
            return pm?.PmId;
        }
        catch (Exception ex) {
            Log.Error($"SourceTable: {table} SourceFilter: {filter} {ex.Message}");
            return null;
        }
    }

    private static void Splash() {
        Console.WriteLine(@"BINGOPARSER.NET: Conversione file TSV e pre-importazione in Giusto! - Nuova versione per .NET 6.0");
        Console.WriteLine(new string('-', 80));
        Console.WriteLine(@"Un'utility per la conversione di file dBase III/FoxPro in testo delimitato e per");
        Console.WriteLine(@"la preimportazione dei dati convertiti in SQL Server");
        Console.WriteLine(@"Opzioni di avvio:");
        Console.WriteLine(@"-h                        Mostra soltanto queste istruzioni; non fare altro.");
        Console.WriteLine(@"-in=<DirIn>               directory di input - default: la directory corrente");
        Console.WriteLine(@"-out=<DirOut>             directory di output - default: uguale a DirIn");
        Console.WriteLine(@"-f=<file>                 nome del file o dei file da convertire - default: *.DBF; *.TXT; *.CSV");
        Console.WriteLine(@"-s=<separator>            separatore di campo - default: tab \t");
        Console.WriteLine(@"-q                        be quiet - messaggi di avanzamento soppressi");
        Console.WriteLine(@"-all                      esporta tutte le letture, anche quelle nulle (default: NO)");
        Console.WriteLine(@"-n=<NomeFileNormalizzato> nome del file contenente l'output normalizzato");
        Console.WriteLine(@"-t=<NomeTabellaPreImport> nome della tabella di preimportazione (deve esistere nel database)");
        Console.WriteLine(@"-k                        Svuota la tabella di preimportazione prima di importare");
        Console.WriteLine(@"-c                        Esegui soltanto la conversione dei file nel file normalizzato");
        Console.WriteLine(@"-w                        Esegui soltanto il riversamento nella tabella di preimportazione");
        Console.WriteLine(new string('-', 80));
        Console.WriteLine(@"Daniele Prevato 2016-2022");
        Console.WriteLine(@"versione 1.0: (2016) Versione iniziale.");
        Console.WriteLine(@"versione 1.2: (2016) Aggiunta la creazione di un file di testo normalizzato per le misure provenienti");
        Console.WriteLine(@"                     dai vari impianti.");
        Console.WriteLine(@"versione 2.0: (2017) Aggiunta la funzionalità di preimportazione");
        Console.WriteLine(@"versione 3.0: (2019) Implementato il logging degli eventi di applicazione;");
        Console.WriteLine(@"                     Generalizzato l'input: qualsiasi file di testo (default: *.txt;*.csv;*.dbf) viene letto");
        Console.WriteLine(@"                     alla ricerca di misure valide.");
        Console.WriteLine(@"versione 3.1: (2019) Aggiunta la data di preimportazione;");
        Console.WriteLine(@"                     Il file tab-delimited di passaggio dei dati viene conservato al termine della procedura.");
        Console.WriteLine(@"versione 4.0: (2019) Rinnovata la procedura di preimportazione, con inserimento del codice PdM come discriminante.");
        Console.WriteLine(@"                     La preimportazione è limitata ai PdM registrati come autoimportati nel database dell'applicazione.'");
        Console.WriteLine(@"versione 4.1: (2020) La tabella di preimportazione viene vuotata prima della preimportazione.");
        Console.WriteLine(@"versione 6.0: (2022) L'applicazione è stata portata alla versione .NET 6.0");
        Console.WriteLine();
    }

    public static void ConvertAllInputFiles() {
        var inputFiles = GetInputFileList(InputDirectory!);
        Log.Information($"Inizio conversione files nella directory {InputDirectory}.");

        if (inputFiles.Count == 0) {
            // ReSharper disable once LocalizableElement
            Console.WriteLine($"Nella directory {InputDirectory} non esistono file del tipo richiesto.");
            Log.Error($"Nessun file del tipo richiesto nella directory {InputDirectory}");
            return;
        }

        Log.Information($"Trovati {inputFiles.Count} files nella directory {InputDirectory}");
        OutputStream = SetOutputStreamWriter($"{OutputDirectory}{OutputFile}");

        foreach (var file in inputFiles) {
            if (new FileInfo(file).Length == 0) continue; // scarto i file vuoti

            if (file.EndsWith(".DBF", StringComparison.InvariantCultureIgnoreCase))
                ConvertDBFFile(file);
            else
                ConvertSingleTextFile(file);
        }

        OutputStream!.Flush();
        OutputStream.Close();
        // ReSharper disable once LocalizableElement
        if (!Quiet) Console.WriteLine($"Sono state convertite in totale {TotalRows} righe.");

        Log.Information($"Conversione terminata - convertite {TotalRows} righe.\n");
    }

    public static List<string> GetInputFileList(string inputDir) {
        var inputFiles = new List<string>();

        foreach (var p in FilePatterns!) inputFiles.AddRange(Directory.GetFiles(inputDir, p));

        Log.Information($"Trovati {inputFiles.Count} files nella directory di input.");
        return inputFiles;
    }

    private static StreamWriter? SetOutputStreamWriter(string dest) {
        var destWriter = new StreamWriter(dest, false); // costante per tutto il ciclo
        destWriter.WriteLine(string.Join(Separator, HeaderData));
        return destWriter;
    }

    private static void ConvertDBFFile(string file) {
        var tbl = DBF.GetDBFSimple(file);
        var fileName = Path.GetFileNameWithoutExtension(file); // Questo devo cercarlo in PmMap
        var rTaken = DateTime.Today.Date.ToString("yyyy-MM-dd");
        var sb = new StringBuilder();
        RowsWritten = 0;
        // Ho ottenuto una tabella contenente i dati presenti nel file passato come parametro. A questo punto, devo trasferire i dati dalla tabella
        // al file di preimportazione

        if (!Quiet) UpdateConsole($"\nConversione da {fileName}", RowsWritten);
        foreach (DataRow item in tbl.Rows) {
            // 1.   Prendo la data che si trova nella colonna DT e verifico che si tratti di una data valida (costante per tutta la riga)
            var rdt = FormatDateForSql(item["DT"].ToString()?.Trim()) ?? FormatDateForSql(item[0].ToString()?.Trim(), item[1].ToString()?.Trim());
            if (string.IsNullOrWhiteSpace(rdt)) continue;

            // 2.   Rilevo il codice PM corrispondente alla colonna corrente
            for (var c = 2; c < tbl.Columns.Count - 3; c++) {
                var rValue = GetValue(item[c].ToString()!.Trim());
                if (string.IsNullOrWhiteSpace(rValue) && ExcludeNulls) continue; // Se il valore è nullo scarto la lettura
                var pm = GetPm(fileName, item.Table.Columns[c].ColumnName); // ricavo il Pm a cui si riferisce la lettura corrente
                if (string.IsNullOrWhiteSpace(pm)) continue;
                string? rText = null;
                // Carico i dati nello stringBuilder
                sb.Append(pm).Append(Separator);
                sb.Append(rdt).Append(Separator);
                sb.Append(rText).Append(Separator);
                sb.Append(rValue).Append(Separator);
                sb.AppendLine(rTaken);
                RowsWritten++;
            }
            if (RowsWritten % 1000 == 0 && !Quiet) UpdateConsole($"Conversione da {fileName}", RowsWritten);
        }
        if (!Quiet) UpdateConsole($"Conversione da {fileName}", RowsWritten);
        OutputStream!.Write(sb);
        TotalRows += RowsWritten;
        Log.Information($"Convertite {RowsWritten} righe dal file {file} (righe totali {TotalRows})");
    }

    public static void UpdateConsole(string message, long rows) => Console.Write(rows == 0 ? $"\r{message}" : $"\r{message}: {rows,12:D}");

    private static void ConvertSingleTextFile(string file) {
        // In una futura versione, il codice può essere generalizzato specificando o ricavando dal file stesso informazioni come
        // numero di campi, separatore, header, ecc.
        // Ora il separatore è TAB, e i tracciati degli unici due file che contengono dati TSV sono i seguenti:
        // Site - Device - DateTime - GaugeValue - GaugeTaken
        // che sono le prime quattro colonne del file di destinazione. La altre colonne sono comunque aggiunte dal programma.
        var fileName = Path.GetFileNameWithoutExtension(file);
        RowsWritten = 0;
        if (!Quiet) UpdateConsole(fileName, RowsWritten);

        var today = DateTime.Today.ToString("yyyy-MM-dd");
        var sb = new StringBuilder();
        using var source = new StreamReader(file);
        while (!source.EndOfStream) {
            var line = source.ReadLine();
            if (string.IsNullOrWhiteSpace(line) || !Regex.IsMatch(line, RowExpectedFormat)) continue;
            line = GetCleanDataRow(line); // Contiene 4 campi gia separati da tab: Channel, GaugeDateTime, RawValue, GaugeValue
            var fieldData = line.Split(new string[] { Separator! }, StringSplitOptions.None).ToList();
            var PdmId = fieldData[0];
            if (PdmId.IsNullValue() || fieldData[3].IsNullValue()) continue; // GetPm non ha restituito un valore per il codice PdM; non preimporto

            var rdt = FormatDateForSql(fieldData[1]);
            if (rdt.IsNullValue()) continue; // scarto le date non valide
            // Costruzione della riga di dati in output
            sb.Append(PdmId).Append(Separator);
            sb.Append(rdt).Append(Separator);
            sb.Append(fieldData[2]).Append(Separator);
            sb.Append(fieldData[3]).Append(Separator);
            sb.AppendLine(today); // La data di presa lettura

            RowsWritten++;
            //OutputStream!.WriteLine(sb.ToString());

            if (RowsWritten % 1000 == 0 && !Quiet) UpdateConsole(fileName, RowsWritten);
        }
        OutputStream!.Write(sb);
        TotalRows += RowsWritten;

        if (!Quiet) {
            UpdateConsole(fileName, RowsWritten);
            Console.WriteLine();
        }

        OutputStream?.Flush();

        Log.Information($"Convertito il file {file} - righe scritte {RowsWritten} (righe totali {TotalRows}).");
    }

    public static string? FormatDateForSql(string? date, string? time) {
        if (string.IsNullOrEmpty(date) || string.IsNullOrEmpty(time)) {
            Log.Error("Formattazione data per SQL fallita, almeno uno dei due parametri è nullo");
            return null;
        }

        if (date.IsIsoDateTime()) return FormatDateForSql(date);

        if (date.IsAnyDate() && time.IsAnyTime())
            return $"{DateTime.ParseExact(date.Substring(0, 10), "dd/MM/yyyy", LocCulture):yyyy-MM-dd} {time.Substring(0, 5).Replace('.', ':')}";
        return null;
    }

    public static string? FormatDateForSql(string? datetime) {
        try {
            if (string.IsNullOrWhiteSpace(datetime!.Trim()) || !datetime.IsIsoDateTime()) return null;

            // minimo aaaaMMdd
            datetime = datetime.Strip("-/.T:");
            var datePart = datetime.Substring(0, 8);
            var timePart = datetime.Remove(0, 8).Insert(4, ":").Insert(2, ":");
            if (timePart.EndsWith(":")) timePart = timePart.Remove(timePart.Length - 1, 1);
            return $"{DateTime.ParseExact(datePart, "yyyyMMdd", CultureInfo.InvariantCulture):yyyy-MM-dd} {timePart}";
        }
        catch (Exception) {
            return null;
        }
    }

    public static string GetCleanDataRow(string dataRow) {
        dataRow = dataRow.Replace("-99999", null)
                         .Replace("-999", null)
                         .Replace("****", null)
                         .Replace(",", ".")
                         .Replace(" ", "")
                         .Replace("-999.00", null);

        if (dataRow.EndsWith("\t")) dataRow = dataRow.Remove(dataRow.Length - 1);
        return dataRow;
    }

    public static string? GetValue(string? value) {
        //return value?.Trim().Replace("-99999", string.Empty)
        //            .Replace("-999", string.Empty)
        //            .Replace("****", string.Empty)
        //            .Replace(",", ".")
        //            .Replace(" ", "")
        //            .Replace("-999.00", string.Empty);
        if (value != null && (value.Contains("-999") || value.Contains("**"))) return null;
        return value?.Replace(",", ".").Trim();
    }
}