// Grazie a Brian Duke per l'articolo "Load a DBF into a DataTable" pubblicato su CodeProject il 11 marzo 2008

using System.Collections;
using System.Data;
using System.Runtime.InteropServices;
using System.Text;

// ReSharper disable MemberCanBePrivate.Local
// ReSharper disable FieldCanBeMadeReadOnly.Local

namespace BingoParser;

public partial class DBF
{
    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi, Pack = 1)]
    private struct DBFHeader
    {
        public byte version;
        public byte updateYear;
        public byte updateMonth;
        public byte updateDay;
        public Int32 numRecords;
        public Int16 headerLen;
        public Int16 recordLen;
        public Int16 reserved1;
        public byte incompleteTrans;
        public byte encryptionFlag;
        public Int32 reserved2;
        public Int64 reserved3;
        public byte MDX;
        public byte language;
        public Int16 reserved4;
    }

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi, Pack = 1)]
    private struct FieldDescriptor
    {
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = 11)]
        public string fieldName;
        public char fieldType;
        public Int32 address;
        public byte fieldLen;
        public byte count;
        public Int16 reserved1;
        public byte workArea;
        public Int16 reserved2;
        public byte flag;
        [MarshalAs(UnmanagedType.ByValArray, SizeConst = 7)]
        public byte[] reserved3;
        public byte indexFlag;
    }

    public DataTable GetDBF(string file) {
        long start = DateTime.Now.Ticks;
        var dt = new DataTable();
        BinaryReader rowReader;
        string number, year, month, day;
        long lDate, lTime;
        DataRow row;
        int fieldIx;

        if (!File.Exists(file)) return dt;

        BinaryReader br = null;

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

            // Blocco 3: Leggo la prima riga di dati; serve per determinare i tipi di dati delle colonne, più avanti
            ((FileStream)br.BaseStream).Seek(header.headerLen + 1, SeekOrigin.Begin);
            buffer = br.ReadBytes(header.recordLen);
            rowReader = new BinaryReader(new MemoryStream(buffer));
            DataColumn col = null!;

            foreach (FieldDescriptor field in fields) {
                number = Encoding.ASCII.GetString(rowReader.ReadBytes(field.fieldLen));
                switch (field.fieldType) {
                    case 'N':
                        if (number.IndexOf(".", StringComparison.Ordinal) > -1) col = new DataColumn(field.fieldName, typeof(decimal));
                        else col = new DataColumn(field.fieldName, typeof(int));
                        break;
                    case 'C':
                        col = new DataColumn(field.fieldName, typeof(string));
                        break;
                    case 'T':
                        // You can uncomment this to see the time component in the grid
                        //col = new DataColumn(field.fieldName, typeof(string));
                        col = new DataColumn(field.fieldName, typeof(DateTime));
                        break;
                    case 'D':
                        col = new DataColumn(field.fieldName, typeof(DateTime));
                        break;
                    case 'L':
                        col = new DataColumn(field.fieldName, typeof(bool));
                        break;
                    case 'F':
                        col = new DataColumn(field.fieldName, typeof(double));
                        break;
                }

                dt.Columns.Add(col);
            }

            // Blocco 4: mi porto all'inizio della zona dati, e leggo tutte le righe in altrettanti oggetti DataRow che aggiungo alla tabella
            ((FileStream)br.BaseStream).Seek(header.headerLen, SeekOrigin.Begin);

            for (int c = 0; c < header.numRecords; c++) {
                buffer = br.ReadBytes(header.recordLen);
                rowReader = new BinaryReader(new MemoryStream(buffer));
                if (rowReader.ReadChar() == '*') continue;

                fieldIx = 0;
                row = dt.NewRow();

                // Blocco 5: per ogni attributo della riga di dati, rilevo il tipo di dati e converto opportunamente i byte letti
                foreach (FieldDescriptor field in fields) {
                    switch (field.fieldType) {
                        case 'N': // number
                            number = Encoding.ASCII.GetString(rowReader.ReadBytes(field.fieldLen));
                            if (decimal.TryParse(number, out var decVal))
                                row[fieldIx] = decVal;
                            else if (int.TryParse(number, out var intVal))
                                row[fieldIx] = intVal;
                            else
                                row[fieldIx] = 0;
                            break;
                        case 'C':  // string
                            row[fieldIx] = Encoding.ASCII.GetString(rowReader.ReadBytes(field.fieldLen));
                            break;
                        case 'D':  // Date (yyyyMMdd)
                            year = Encoding.ASCII.GetString(rowReader.ReadBytes(4));
                            month = Encoding.ASCII.GetString(rowReader.ReadBytes(2));
                            day = Encoding.ASCII.GetString(rowReader.ReadBytes(2));
                            row[fieldIx] = DBNull.Value;
                            try {
                                int.TryParse(year, out var y); // se fallisce, amen
                                int.TryParse(month, out var m); // anche questo
                                int.TryParse(day, out var d);
                                row[fieldIx] = new DateOnly(y, m, d);
                            }
                            catch { }

                            break;
                        case 'T': // Timestamp - 8 bytes, i primi due per la data e gli altri due per l'ora
                            // La data è il numero di giorni trascorsi dal 01/01/4713 A.C. (giorni Giuliani)
                            // l'ora è Ore * 3_600_000L + Minuti * 60_000L + Secondi * 1_000L (Millisecondi a partire dalla mezzanotte)
                            lDate = rowReader.ReadInt32();
                            lTime = rowReader.ReadInt32() * 10_000L;
                            row[fieldIx] = JulianToDateTime(lDate).AddTicks(lTime);
                            break;
                        case 'L': // Boolean (Y/N T/F V/F)
                            if ("YTV".Contains(rowReader.ReadChar()))
                                row[fieldIx] = true;
                            else
                                row[fieldIx] = false;
                            break;
                        case 'F':
                            number = Encoding.ASCII.GetString(rowReader.ReadBytes(field.fieldLen));
                            if (double.TryParse(number, out var n))
                                row[fieldIx] = n;
                            else
                                row[fieldIx] = 0.0F;
                            break;
                    }
                    fieldIx++;
                }
                rowReader.Close();
                dt.Rows.Add(row);
            }
        }
        catch {
            throw;
        }
        finally {
            if (br is not null) br.Close();
        }
        long count = DateTime.Now.Ticks - start;
        return dt;
    }


    private static DateTime JulianToDateTime(long JDN) {
        double p = Convert.ToDouble(JDN);
        double s1 = p + 68_569;
        double n = Math.Floor(4 * s1 / 146_097);
        double s2 = s1 - Math.Floor((146_097 * n + 3) / 4);
        double i = Math.Floor(4_000 * (s2 + 1) / 1_461_001);
        double s3 = s2 - Math.Floor(1_461 * i / 4) + 31;
        double q = Math.Floor(80 * s3 / 2_447);
        double d = s3 - Math.Floor(2_447 * q / 80);
        double s4 = Math.Floor(q / 11);
        double m = q + 2 - 12 * s4;
        double j = 100 * (n - 49) + i + s4;
        return new DateTime(Convert.ToInt32(j), Convert.ToInt32(m), Convert.ToInt32(d));
    }
}
