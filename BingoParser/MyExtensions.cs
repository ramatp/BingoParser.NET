using System.Text.RegularExpressions;

namespace BingoParser;

public static class MyExtensions
{
    /// <summary>
    /// Elimina gli spazi da una stringa di testo
    /// </summary>
    /// <param name="s"></param>
    /// <returns>La stringa priva degli spazi rimossi</returns>
    public static string Strip(this string s) {
        return s.Replace(" ", string.Empty);
    }

    /// <summary>
    /// Elimina tutti i caratteri paassati nell'array c
    /// </summary>
    /// <param name="s">stringa da midificare</param>
    /// <param name="c">array di caratteri da eliminare</param>
    /// <returns>la stringa ripulita</returns>
    public static string Strip(this string s, string c) {
        foreach (var t in c) {
            s = s.Replace(t, char.MinValue);
        }

        return s.Strip();
    }

    /// <summary>
    /// Verifica se una variabile string vale null, è vuota, o se contiene soltanto spazi.
    /// </summary>
    /// <param name="s"></param>
    /// <returns>true se la stringa non contiene caratteri significativi</returns>
    public static bool IsNullValue(this string? s) {
        return s is null || string.IsNullOrWhiteSpace(s.Trim());
    }

    public static bool IsIsoDate(this string s) {
        return Regex.IsMatch(s, @"^(19|20)\d\d[-_ /.]?(0[1-9]|1[012])[-_ /.]?(0[1-9]|[12][0-9]|3[01])$");
    }

    public static bool IsIsoDateTime(this string s) {
        return Regex.IsMatch(s, @"^(19|20)\d\d[-_ /.]?(0[1-9]|1[012])[-_ /.]?(0[1-9]|[12][0-9]|3[01])[ T]?([01][0-9]|2[0-3])[:.]?([0-5][0-9])[:.]?([0-5][0-9])?$");
    }

    public static bool IsAnyTime(this string t) {
        return Regex.IsMatch(t, @"^([0-5][0-9][.:]?)([0-5][0-9])([.:]?[0-5][0-9])?$");
    }

    public static bool IsAnyDateTime(this string s) {
        return IsIsoDateTime(s) ||
               Regex.IsMatch(s, @"^([0-3][1-9][-/.])(0[1-9]|1[012])[-/.](19|20)\d{2}[ T-]([0-5][0-9])[:.]([0-5][0-9])([:.][0-5][0-9])?$") ||
               Regex.IsMatch(s, @"^(19|20)\d{2}[\.-/]?(0[1-9]|1[0-2])[\.-/]?(0[1-9]|[1-2][0-9]|[3][01])([ T]?([0-5][0-9][.:]?)([0-5][0-9])([.:]?[0-5][0-9])?)?$");
    }

    public static bool IsAnyDate(this string s) {
        return IsIsoDate(s) ||
               Regex.IsMatch(s, @"^([0-3][1-9][-/.])(0[1-9]|1[012])[-/.](19|20)\d{2}.*$") ||
               Regex.IsMatch(s, @"^(19|20)\d{2}[\.-/]?(0[1-9]|1[0-2])[\.-/]?(0[1-9]|[1-2][0-9]|[3][01])$");
    }

}