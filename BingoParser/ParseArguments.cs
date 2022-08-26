using Serilog;
using System.Text.RegularExpressions;

namespace BingoParser;
public static class ParseArguments
{
    public static string? InputDirectory { get; private set; }
    public static string? OutputDirectory { get; private set; }
    public static string? FilePattern { get; private set; }
    public static List<string>? FilePatterns { get; set; }
    public static string? Separator { get; private set; }
    public static bool Quiet { get; private set; }
    public static string? OutputFile { get; private set; }
    public static bool ExcludeNulls { get; private set; }
    public static string? BulkImportTableName { get; private set; }
    public static bool BulkImportOnly { get; private set; }
    public static bool ConvertOnly { get; private set; }
    public static bool NewImport { get; private set; }
    public static string? ConnectionString { get; private set; }
    public static bool Help { get; private set; }

    /// <summary>
    /// Riceve il vettore di opzioni della riga di comando, lo interpreta e assegna i valori ai membri
    /// </summary>
    /// <param name="args"></param>
    public static void Parse(string[] args) {
        // Così posso semplificare i test successivi
        for (var x = 0; x < args.Length; x++) args[x] = args[x].Strip().ToLowerInvariant();

        // Directory di input
#if DEBUG
        InputDirectory = @"d:\Repository\SQLServer\Data\";
#else
        InputDirectory = @".\"; //  Questo è il valore di default, che rimane se nel ciclo non succede niente
#endif
        Help = false;
        foreach (var v in args) {
            if (Regex.IsMatch(v, @"^[-/]h$")) Help = true;
        }
        foreach (var v in args) {
            if (!Regex.IsMatch(v, @"^[-/]in=.+$")) continue;
            if (Directory.Exists(v.Substring(4))) InputDirectory = v.Substring(4);
        }
        if (!InputDirectory.EndsWith(@"\")) InputDirectory += @"\";

        OutputDirectory = $@"{InputDirectory}"; // Il file di destinazione viene scritto nella stessa directory dove si trovano i file di input
        foreach (var v in args) {
            if (Regex.IsMatch(v, @"^[-/]out=.+$")) OutputDirectory = v.Substring(5); // non è necessario fare il test su maiuscole e minuscole; è stato tutto trasfromato in minuscole
        }
        if (!OutputDirectory.EndsWith(@"\")) OutputDirectory += @"\";

        OutputFile = $@"AllReadings.tsv";
        foreach (var v in args) {
            if (!Regex.IsMatch(v, @"^[-/]n=")) continue;
            OutputFile = v.Substring(3);
            if (!OutputFile.EndsWith(@".tsv", StringComparison.InvariantCultureIgnoreCase)) OutputFile += ".tsv";
        }

        FilePattern = @"*.dbf;*.txt;*.csv";
        foreach (var v in args) {
            if (!Regex.IsMatch(v, @"^[-/]f=")) continue;
            FilePattern = $"{v.Strip().Substring(3)}";
        }
        FilePatterns = FilePattern.Strip().Split(';').ToList();
        Log.Information($"Estensioni selezionate: {FilePattern}");

        Separator = "\t";
        foreach (var v in args) {
            if (!Regex.IsMatch(v, @"^[-/]s=")) continue;
            Separator = v.Substring(3);
        }

        Quiet = false;
        foreach (var v in args) {
            if (Regex.IsMatch(v, @"^[-/]q")) Quiet = true;
        }

        ExcludeNulls = true;
        foreach (var v in args) {
            if (!Regex.IsMatch(v, @"^[-/]all")) ExcludeNulls = false;
        }

        BulkImportTableName = "Misure.Preimport";
        foreach (var v in args) {
            if (Regex.IsMatch(v, @"^[-/]t=.+$")) BulkImportTableName = v.Substring(3);
        }

        BulkImportOnly = false;
        foreach (var v in args) {
            if (Regex.IsMatch(v, @"^[-/]k$")) BulkImportOnly = true;
        }

        ConvertOnly = false;
        foreach (var v in args) {
            if (Regex.IsMatch(v, @"^[-/]c$")) ConvertOnly = true;
        }

        NewImport = false;
        foreach (var v in args) {
            if (Regex.IsMatch(v, @"^[-/]w$")) NewImport = true;
        }
    }
}