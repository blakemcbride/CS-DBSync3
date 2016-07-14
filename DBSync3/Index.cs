using System;
using System.Collections.Generic;
using System.Text;

namespace DBSync3
{
    class Index : ISchemaElement
    {
        private string table;
        private string iname;
        private bool only;
        private string type;
        private LinkedList<string> fields;
        private bool isDeleted;
        private bool isIndex;

        public Index(string nam, string tab, bool on, string typ, bool isIdx, LinkedList<string> flds)
        {
            iname = nam;
            table = tab;
            only = on;
            type = typ;
            fields = flds;
            isIndex = isIdx;
            isDeleted = false;
        }

        public override bool Equals(object other)
        {
            return Equals(this, other as Index);
        }

        public static bool Equals(Index a, Index b)
        {
            if (string.Compare(a.table, b.table, true) != 0 ||
                string.Compare(a.iname, b.iname, true) != 0 ||
                string.Compare(a.type, b.type, true) != 0 ||
                a.fields.Count != b.fields.Count ||
                a.isIndex != b.isIndex  ||
                a.isDeleted != b.isDeleted)
                return false;
            LinkedListNode<string>  na = a.fields.First;
            LinkedListNode<string>  nb = b.fields.First;
            for (int i = 0; i < a.fields.Count; i++)
            {
                if (string.Compare(na.Value, nb.Value, true) != 0)
                    return false;
                na = na.Next;
                nb = nb.Next;
            }
            return true;
        }

        public static bool operator ==(Index a, Index b)
        {
            return Equals(a, b);
        }

        public static bool operator !=(Index a, Index b)
        {
            return !Equals(a, b);
        }

        public override int GetHashCode()
        {
            return iname.GetHashCode();
        }

        public void set_type(string typ)
        {
            type = typ;
        }

        public string get_type()
        {
            return type;
        }

        public string get_name()
        {
            return iname;
        }

        public bool isElement(string field)
        {
            foreach (string fld in fields)
                if (field == fld)
                    return true;
            return false;
        }

        public void add_postgres()
        {
            bool comma = false;
            if (isIndex)
            {
                if (type == "UNIQUE")
                    Console.Write("CREATE UNIQUE INDEX \"" + iname + "\" ON \"" + table + "\" USING btree (");
                else
                    Console.Write("CREATE INDEX \"" + iname + "\" ON \"" + table + "\" USING btree (");
                foreach (string fld in fields)
                {
                    if (comma)
                        Console.Write(", ");
                    else
                        comma = true;
                    if (fld.StartsWith("upper(") || fld.StartsWith("lower("))
                    {
                        Console.Write(fld.Substring(0, 6) + "\"" + fld.Substring(6, fld.Length - 7) + "\")");
                    }
                    else
                        Console.Write("\"" + fld + "\"");
                }
                Console.WriteLine(");\n");
            }
            else
            {
                Console.Write("ALTER TABLE ONLY \"" + table + "\" ADD CONSTRAINT \"" + iname + "\" " + type + " (");
                foreach (string fld in fields)
                {
                    if (comma)
                        Console.Write(", ");
                    else
                        comma = true;
                    Console.Write("\"" + fld + "\"");
                }
                Console.WriteLine(");\n");
            }
        }

        public void add_microsoft()
        {
            bool comma = false;
            if (isIndex)
            {
                if (type == "UNIQUE")
                    Console.Write("CREATE UNIQUE INDEX [" + iname + "] ON [" + table + "] (");
                else
                    Console.Write("CREATE INDEX [" + iname + "] ON [" + table + "] (");
                foreach (string fld in fields)
                {
                    if (comma)
                        Console.Write(", ");
                    else
                        comma = true;
                    if (fld.StartsWith("upper(") || fld.StartsWith("lower("))
                    {
                        Console.Write("[" + fld.Substring(6, fld.Length - 7) + "]");
                    }
                    else
                       Console.Write("[" + fld + "]");
                }
                Console.WriteLine(")\nGO\n");
            }
            else
            {
                Console.Write("ALTER TABLE [" + table + "] ADD CONSTRAINT [" + iname + "] " + type + " (");
                foreach (string fld in fields)
                {
                    if (comma)
                        Console.Write(", ");
                    else
                        comma = true;
                    Console.Write("[" + fld + "]");
                }
                Console.WriteLine(")\nGO\n");
            }
        }

        public void add_oracle()
        {
            bool comma = false;
            if (isIndex)
            {
                if (type == "UNIQUE")
                    Console.Write("CREATE UNIQUE INDEX \"" + DBSync3.limit_name(30, iname.ToUpper()) + "\" ON \"" + table.ToUpper() + "\" (");
                else
                    Console.Write("CREATE INDEX \"" + DBSync3.limit_name(30, iname.ToUpper()) + "\" ON \"" + table.ToUpper() + "\" (");
                foreach (string fld in fields)
                {
                    if (comma)
                        Console.Write(", ");
                    else
                        comma = true;
                    if (fld.StartsWith("upper(") || fld.StartsWith("lower("))
                    {
                        Console.Write(fld.Substring(0, 6).ToUpper() + "\"" + fld.Substring(6, fld.Length - 7).ToUpper() + "\")");
                    }
                    else
                        Console.Write("\"" + fld.ToUpper() + "\"");
                }
                Console.WriteLine(");\n");
            }
            else
            {
                Console.Write("ALTER TABLE \"" + table.ToUpper() + "\" ADD CONSTRAINT \"" + DBSync3.limit_name(30, iname.ToUpper()) + "\" " + type + " (");
                foreach (string fld in fields)
                {
                    if (comma)
                        Console.Write(", ");
                    else
                        comma = true;
                    Console.Write("\"" + fld.ToUpper() + "\"");
                }
                Console.WriteLine(");\n");
            }
        }

        public void drop_postgres()
        {
            if (isDeleted)
                return;
            else
                isDeleted = true;
            if (isIndex)
            {
                Console.WriteLine("DROP INDEX \"" + iname + "\";\n");
            }
            else
            {
                Console.WriteLine("ALTER TABLE ONLY \"" + table + "\" DROP CONSTRAINT \"" + iname + "\";\n");
            }
        }

        public void drop_microsoft()
        {
            if (isDeleted)
                return;
            else
                isDeleted = true;
            if (isIndex)
            {
                Console.WriteLine("DROP INDEX [" + table + "].[" + iname + "]\nGO\n");
            }
            else
            {
                Console.WriteLine("ALTER TABLE [" + table + "] DROP CONSTRAINT [" + iname + "]\nGO\n");
            }
        }

    }
}
