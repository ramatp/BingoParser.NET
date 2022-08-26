using Microsoft.Data.SqlClient;
using RepoDb;
using System.Diagnostics;
using System.Configuration;

namespace BingoParser;
public static class Connector
{
    private static string ConnectionString { get; }

    static Connector() {
        ConnectionString = Debugger.IsAttached ?
                               ConfigurationManager.ConnectionStrings["LocalDb"].ConnectionString :
                               ConfigurationManager.ConnectionStrings["ProductionDb"].ConnectionString;
    }

    public static Func<SqlConnection?> CreateConnection = () => {
        using var connection = Activator.CreateInstance<SqlConnection>();
        connection.ConnectionString = ConnectionString;
        return connection.EnsureOpen() as SqlConnection;
    };

    public static SqlConnection Create() {
        return (SqlConnection)new SqlConnection(ConnectionString).EnsureOpen();
    }
}
