using System;
using System.Collections.Generic;
using System.Text;

namespace DBSync3
{
    class Check : ISchemaElement
    {
        private string table;
        private string name;
        private LinkedList<string> constraint;
        private int dbtype;
        private bool isDeleted;

        public Check(int dbtyp, string nam, string tname)
        {
            dbtype = dbtyp;
            name = nam;
            table = tname;
            isDeleted = false;
        }

        public override bool Equals(object other)
        {
            return Equals(this, other as Check);
        }

        public static bool Equals(Check a, Check b)
        {
            if (string.Compare(a.table, b.table, true) != 0 ||
                string.Compare(a.name, b.name, true) != 0  ||
                a.isDeleted != b.isDeleted)
                return false;

            LinkedListNode<string> an = a.constraint.First;
            LinkedListNode<string> bn = b.constraint.First;
            while (true)
            {
                while (an != null && (an.Value == "(" || an.Value == ")"))
                    an = an.Next;
                while (bn != null && (bn.Value == "(" || bn.Value == ")"))
                    bn = bn.Next;
                if (an == null && bn != null || an != null && bn == null)
                    return false;
                if (an == null && bn == null)
                    return true;
                string sa = an.Value;
                string sb = bn.Value;
                if (sa[0] == '\'')
                {
                    if (string.Compare(sa, sb) != 0)
                        return false;
                }
                else
                    if (string.Compare(sa, sb, true) != 0)
                        return false;
                an = an.Next;
                bn = bn.Next;
            }
        }

        public static bool operator ==(Check a, Check b)
        {
            return Equals(a, b);
        }

        public static bool operator !=(Check a, Check b)
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

        public void set_constraint(LinkedList<string> typ)
        {
            constraint = typ;
        }

        public LinkedList<string> get_constraint()
        {
            return constraint;
        }

        public bool isElement(string field)
        {
            foreach (string fld in constraint)
                if (field == fld)
                    return true;
            return false;
        }

        public void add_inside_postgres()
        {
            Console.Write("CONSTRAINT \"" + name + "\" CHECK " + DBSync3.list_to_string(constraint));
        }

        public void add_inside_microsoft()
        {
            Console.Write("CONSTRAINT [" + name + "] CHECK " + DBSync3.list_to_string(constraint));
        }

        private LinkedList<string> fix_oracle_check(LinkedList<string> con)
        {
            LinkedList<string> res = new LinkedList<string>();
            LinkedListNode<string> lnk;
            for (lnk = con.First; lnk != null; lnk = lnk.Next)
            {
                if (char.IsLetter(lnk.Value[0])  &&  lnk.Value != "OR"  &&  lnk.Value != "AND")
                    res.AddLast("\"" + lnk.Value.ToUpper() + "\"");
                else
                    res.AddLast(lnk.Value);
            }
            return res;
        }

        public void add_inside_oracle()
        {
            Console.Write("CONSTRAINT \"" + name.ToUpper() + "\" CHECK " + DBSync3.list_to_string(fix_oracle_check(constraint)));
        }

        public void add_postgres()
        {
            Console.WriteLine("ALTER TABLE \"" + table + "\" ADD CONSTRAINT \"" + name + "\" CHECK " + DBSync3.list_to_string(constraint)
                + ";\n");
        }

        public void add_microsoft()
        {
            Console.WriteLine("ALTER TABLE [" + table + "] ADD CONSTRAINT [" + name + "] CHECK " + DBSync3.list_to_string(constraint)
                + "\nGO\n");
        }

        public void add_oracle()
        {
            Console.WriteLine("ALTER TABLE \"" + table.ToUpper() + "\" ADD CONSTRAINT \"" + DBSync3.limit_name(30, name.ToUpper()) + "\" CHECK " + DBSync3.list_to_string(constraint)
                + ";\n");
        }

        public void drop_postgres()
        {
            if (!isDeleted)
            {
                Console.WriteLine("ALTER TABLE \"" + table + "\" DROP CONSTRAINT \"" + name + "\";\n");
                isDeleted = true;
            }
        }

        public void drop_microsoft()
        {
            if (!isDeleted)
            {
                Console.WriteLine("ALTER TABLE [" + table + "] DROP CONSTRAINT [" + name + "]\nGO\n");
                isDeleted = true;
            }
        }

        public static void alter_check(Check oldcol, Check newcol)
        {
            if (oldcol.dbtype == Schema.Postgres)
            {
                oldcol.drop_postgres();
                newcol.add_postgres();
            }
            else if (oldcol.dbtype == Schema.Microsoft)
            {
                oldcol.drop_microsoft();
                newcol.add_microsoft();
            }
        }

    }
}
