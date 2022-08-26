using RepoDb.Attributes;

namespace BingoParser.Model;
[Map("dbo.T30_Import")]
public class PmMapper
{
    [Map("PdmId")] public string PmId { get; set; } = null!;
    public string SourceTable { get; set; } = null!;
    public string SourceFilter { get; set; } = null!;
}
