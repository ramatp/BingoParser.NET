using RepoDb.Attributes;

namespace BingoParser.Model;

[Map("Misure.Preimport")]
public class PreImport
{
    [Map("PdmId")] public string PmId { get; set; } = null!;
    public DateTime RDateTime { get; set; }
    public string? RText { get; set; }
    public double RValue { get; set; }
    public DateOnly RTaken { get; set; }
}