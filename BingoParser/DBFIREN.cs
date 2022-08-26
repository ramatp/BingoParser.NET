using System.Collections;
using System.Data;
using System.Runtime.InteropServices;
using System.Text;

namespace BingoParser;
public partial class DBF
{
    public static DataTable GetDBFSimple(string file) {
        var dt = new DataTable();
        BinaryReader rowReader;
        DataRow row;
        int fieldIx;

        if (!File.Exists(file)) return dt;

        BinaryReader br = null!;

        try {
            // Blocco 1: carico l'header del file nella struttura DBFHeader
            br = new BinaryReader(File.OpenRead(file));
            var buffer = br.ReadBytes(Marshal.SizeOf(typeof(DBFHeader)));
            var handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
            var header = (DBFHeader)Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(DBFHeader))!;
            handle.Free();

            // Blocco 2: Carico i descrittori dei campi in un vettore di oggetti FieldDescriptor
            var fields = new ArrayList();
            while (br.PeekChar() != 0x0d) {
                buffer = br.ReadBytes(Marshal.SizeOf(typeof(FieldDescriptor)));
                handle = GCHandle.Alloc(buffer, GCHandleType.Pinned);
                fields.Add((FieldDescriptor)Marshal.PtrToStructure(handle.AddrOfPinnedObject(), typeof(FieldDescriptor))!);
                handle.Free();
            }

            // Blocco 3: molto semplificato; tutte le colonne sono di tipo string
            ((FileStream)br.BaseStream).Seek(header.headerLen + 1, SeekOrigin.Begin);
            buffer = br.ReadBytes(header.recordLen);
            rowReader = new BinaryReader(new MemoryStream(buffer));

            foreach (FieldDescriptor field in fields) {
                dt.Columns.Add(new DataColumn(field.fieldName, typeof(string)));
            }

            // Blocco 5: salto alla fine dell'header e leggo tutte le righe di dati
            ((FileStream)br.BaseStream).Seek(header.headerLen, SeekOrigin.Begin);

            for (var c = 0; c < header.numRecords; c++) {
                buffer = br.ReadBytes(header.recordLen);
                rowReader = new BinaryReader(new MemoryStream(buffer));
                if (rowReader.ReadChar() == '*') continue;

                fieldIx = 0;
                row = dt.NewRow();

                // Non ho bisogno di rilevare il tipo di dati di ogni campo: sono tutti di tipo string
                foreach (FieldDescriptor field in fields) {
                    row[fieldIx] = Program.GetValue(Encoding.ASCII.GetString(rowReader.ReadBytes(field.fieldLen)));
                    fieldIx++;
                }
                rowReader.Close();
                dt.Rows.Add(row);
            }
        }
        catch { throw; }
        finally { br.Close(); }

        return dt;
    }
}
