using System;
using System.Collections.Generic;
using System.Text;

namespace DBSync3
{
    class Column : ISchemaElement
    {
        private string name;
        private string table;
        private LinkedList<string> type;
        private string default_val;
        private string cname;  // constraint name - Microsoft treats default values as constraints
        private bool not_null;
        private int dbtype;

        public Column(int dbtyp, string nm, string tab)
        {
            name = nm;
            table = tab;
            dbtype = dbtyp;
            not_null = false;
        }

        public override bool Equals(object other)
        {
            return Equals(this, other as Column);
        }

        private LinkedList<string> get_fixed_type()
        {
            if (dbtype == Schema.Microsoft)
                return fix_type_microsoft(type);
            return type;
        }

        private static string list_fix_timestamp(LinkedList<string> type, LinkedList<string> type2)
        {
            string r = type.First.Value;
            if (r == "timestamp"  &&  type.Count != type2.Count)
                return r;
            return DBSync3.list_to_string(type);
        }

        public static bool Equals(Column a, Column b)
        {
            string at = list_fix_timestamp(a.type, b.type);
            string bt = list_fix_timestamp(b.type, a.type);
//            Console.WriteLine(a.table + ", " + a.name + " - " + at + " - " + bt);
            return string.Compare(a.name, b.name, true) == 0 &&
                string.Compare(a.table, b.table, true) == 0 &&
//                a.default_val == b.default_val &&
                a.not_null == b.not_null &&
                string.Compare(at, bt, true) == 0;
        }

        public static bool operator ==(Column a, Column b)
        {
            return Equals(a, b);
        }

        public static bool operator !=(Column a, Column b)
        {
            return !Equals(a, b);
        }

        public override int GetHashCode()
        {
            return name.GetHashCode();
        }

        public string get_name()
        {
            return name;
        }

        public string get_default()
        {
            return default_val;
        }

        public void set_type(LinkedList<string> typ)
        {
            type = typ;
        }

        public LinkedList<string> get_type()
        {
            return type;
        }

        public void set_default(string dflt, string constraint)
        {
            default_val = dflt;
            cname = constraint;
        }

        public void set_default(string dflt)
        {
            default_val = dflt;
        }

        public void set_not_null(bool flg)
        {
            not_null = flg;
        }

        public void add_inside_postgres()
        {
            Console.Write("\"" + name + "\" ");
            Console.Write(DBSync3.list_to_string(type));
            if (default_val != null)
                Console.Write(" DEFAULT " + default_val);
            if (not_null)
                Console.Write(" NOT NULL");
        }

        private static LinkedList<string> fix_type_oracle(LinkedList<string> typ)
        {
            if (typ == null)
                return typ;
            LinkedListNode<string> lnk = typ.First;
            string itm = lnk.Value;
            if (itm == null)
                return typ;
            if (itm == "timestamp" ||
                itm == "date")
            {
                LinkedList<string> res = new LinkedList<string>();
                res.AddLast(itm);
                lnk = lnk.Next;
                if (lnk == null)
                    return res;
                itm = lnk.Value;
                if (itm == "without")
                {
//                    res.AddLast("with");
//                    res.AddLast("local");
//                    res.AddLast("time");
//                    res.AddLast("zone");
                    return res;
                }
            }
            if (itm == "character")
            {
                lnk = lnk.Next;
                if (lnk == null)
                    return typ;
                itm = lnk.Value;
                if (itm == "varying")
                {
                    LinkedList<string> res = new LinkedList<string>();
                    res.AddLast("varchar2");
                    for (lnk = lnk.Next; lnk != null; lnk = lnk.Next)
                        res.AddLast(lnk.Value);
                    return res;
                }
            }
            if (itm == "text")
            {
                LinkedList<string> res = new LinkedList<string>();
                res.AddLast("clob");
                return res;
            }
            if (itm == "smallint")
            {
                LinkedList<string> res = new LinkedList<string>();
                res.AddLast("NUMBER(5)");
                return res;
            }
            if (itm == "integer")
            {
                LinkedList<string> res = new LinkedList<string>();
                res.AddLast("NUMBER(10)");
                return res;
            }
            if (itm == "bytea")
            {
                LinkedList<string> res = new LinkedList<string>();
                res.AddLast("blob");
                return res;
            }
            return typ;
        }

        public void add_inside_oracle()
        {
            Console.Write("\"" + name.ToUpper() + "\" ");
            Console.Write(DBSync3.list_to_string(fix_type_oracle(type)));
            if (default_val != null)
                Console.Write(" DEFAULT " + default_val);
            if (not_null)
                Console.Write(" NOT NULL");
        }

        private static LinkedList<string> fix_type_microsoft(LinkedList<string> typ)
        {
            if (typ == null)
                return typ;
            string first = typ.First.Value;
            if (first == null)
                return typ;
            if (first == "timestamp" ||
                first == "date" ||
                first == "time")
            {
                typ = new LinkedList<string>();
                typ.AddLast("datetime");
            }
            if (first == "bytea")
            {
                typ = new LinkedList<string>();
                typ.AddLast("image");
            }
            return typ;
        }

        public void add_inside_microsoft()
        {
            Console.Write("[" + name + "] ");
            Console.Write(DBSync3.list_to_string(fix_type_microsoft(type)));
            if (default_val != null)
                Console.Write(" DEFAULT " + default_val);
            if (not_null)
                Console.Write(" NOT NULL");
        }

        public void add_postgres()
        {
            Console.Write("ALTER TABLE \"" + table + "\" ADD COLUMN \"" + name + "\" " + DBSync3.list_to_string(type));
            if (default_val != null)
                Console.Write(" DEFAULT " + default_val);
            if (not_null)
                Console.Write(" NOT NULL");
            Console.WriteLine(";\n");
        }

        public void add_microsoft()
        {
            Console.Write("ALTER TABLE [" + table + "] ADD [" + name + "] " + DBSync3.list_to_string(fix_type_microsoft(type)));
            if (default_val != null)
                Console.Write(" DEFAULT " + default_val);
            else
                if (not_null)
                {
                    string typ = type.First.Value;
                    if (typ == "character")
                        Console.Write(" DEFAULT ''");
                    else
                        Console.Write(" DEFAULT 0");
                }
            if (not_null)
                Console.Write(" NOT NULL");
            Console.WriteLine("\nGO\n");
        }

        public void add_oracle()
        {
            Console.Write("ALTER TABLE \"" + table.ToUpper() + "\" ADD COLUMN \"" + name.ToUpper() + "\" " + DBSync3.list_to_string(type));
            if (default_val != null)
                Console.Write(" DEFAULT " + default_val);
            if (not_null)
                Console.Write(" NOT NULL");
            Console.WriteLine(";\n");
        }

        public void drop_postgres()
        {
            Console.WriteLine("ALTER TABLE \"" + table + "\" DROP COLUMN \"" + name + "\";\n");
        }

        public void drop_microsoft()
        {
            if (default_val != null  &&  cname != null)
                Console.WriteLine("ALTER TABLE [" + table + "] DROP [" + cname + "]\nGO\n");
            Console.WriteLine("ALTER TABLE [" + table + "] DROP COLUMN [" + name + "]\nGO\n");
        }

        public static void alter_column(Column oldcol, Column newcol)
        {
            if (oldcol.dbtype == Schema.Postgres)
            {
                if (oldcol.not_null == true && newcol.not_null == false)
                    Console.Write("ALTER TABLE \"" + newcol.table + "\" ALTER COLUMN \"" + newcol.name + "\" DROP NOT NULL;\n");
                else if (oldcol.not_null == false && newcol.not_null == true)
                    Console.Write("ALTER TABLE \"" + newcol.table + "\" ALTER COLUMN \"" + newcol.name + "\" SET NOT NULL;\n");
                string at = list_fix_timestamp(oldcol.type, newcol.type);
                string bt = list_fix_timestamp(newcol.type, oldcol.type);
                if (string.Compare(at, bt, true) != 0)
                    Console.Write("ALTER TABLE \"" + newcol.table + "\" ALTER COLUMN \"" + newcol.name + "\" TYPE " +
                        DBSync3.list_to_string(newcol.type) + ";\n");
               
            }
            else if (oldcol.dbtype == Schema.Microsoft)
            {
                Console.Write("ALTER TABLE [" + newcol.table + "] ALTER COLUMN ");
                Console.Write("[" + newcol.name + "] ");
                Console.Write(DBSync3.list_to_string(fix_type_microsoft(newcol.type)));
                if (newcol.not_null)
                    Console.Write(" NOT NULL");
                Console.WriteLine("\nGO\n");
            }
        }

        public static void alter_default(Column oldcol, Column newcol)
        {
            if (oldcol.dbtype == Schema.Postgres)
            {
                Console.Write("ALTER TABLE \"" + newcol.table + "\" ALTER COLUMN \"" + newcol.name + "\" ");
                if (newcol.default_val == null)
                    Console.Write("DROP DEFAULT");
                else
                    Console.Write("SET DEFAULT " + newcol.default_val);
                Console.WriteLine(";\n");
            }
            else if (oldcol.dbtype == Schema.Microsoft)
            {
                if (oldcol.default_val != null && oldcol.cname != null)
                {
                    Console.WriteLine("ALTER TABLE [" + oldcol.table + "] DROP [" + oldcol.cname + "]\nGO\n");
                }
                if (newcol.default_val != null)
                {
                    Console.Write("ALTER TABLE [" + newcol.table + "] ADD CONSTRAINT [");
                    if (newcol.cname == null)
                        newcol.cname = newcol.table + "_" + newcol.name + "_" + "dflt";
                    Console.Write(newcol.cname);
                    Console.WriteLine("] DEFAULT " + newcol.default_val + " FOR [" + newcol.name + "]\nGO\n");
                }
            }
        }
    }
}
