using System;
using System.Collections.Generic;
using System.Text;

namespace DBSync3
{
    class ForeignKey : ISchemaElement
    {
        private string table;
        private string iname;
        private bool only = false;
        private LinkedList<string> fields;
        private string table2;
        private LinkedList<string> fields2;

        public ForeignKey(string nam, string tab, bool on, LinkedList<string> flds, string tab2, LinkedList<string> flds2)
        {
            iname = nam;
            table = tab;
            only = on;
            fields = flds;
            table2 = tab2;
            fields2 = flds2;
        }

        public string get_name()
        {
            return iname;
        }

        public void add_postgres()
        {
            bool comma = false;
            Console.Write("ALTER TABLE ONLY \"" + table + "\" ADD CONSTRAINT \"" + iname + "\" FOREIGN KEY (");
            foreach (string fld in fields)
            {
                if (comma)
                    Console.Write(", ");
                else
                    comma = true;
                Console.Write("\"" + fld + "\"");
            }
            Console.Write(") REFERENCES \"" + table2 + "\" (");
            comma = false;
            foreach (string fld in fields2)
            {
                if (comma)
                    Console.Write(", ");
                else
                    comma = true;
                Console.Write("\"" + fld + "\"");
            }
            Console.WriteLine(");\n");
        }

        public void add_microsoft()
        {
            bool comma = false;
            Console.Write("ALTER TABLE [" + table + "] ADD CONSTRAINT [" + iname + "] FOREIGN KEY (");
            foreach (string fld in fields)
            {
                if (comma)
                    Console.Write(", ");
                else
                    comma = true;
                Console.Write("[" + fld + "]");
            }
            Console.Write(") REFERENCES [" + table2 + "] (");
            comma = false;
            foreach (string fld in fields2)
            {
                if (comma)
                    Console.Write(", ");
                else
                    comma = true;
                Console.Write("[" + fld + "]");
            }
            Console.WriteLine(")\nGO\n");
        }

        public void add_oracle()
        {
            bool comma = false;
            Console.Write("ALTER TABLE \"" + table.ToUpper() + "\" ADD CONSTRAINT \"" + DBSync3.limit_name(30, iname.ToUpper()) + "\" FOREIGN KEY (");
            foreach (string fld in fields)
            {
                if (comma)
                    Console.Write(", ");
                else
                    comma = true;
                Console.Write("\"" + fld.ToUpper() + "\"");
            }
            Console.Write(") REFERENCES \"" + table2.ToUpper() + "\" (");
            comma = false;
            foreach (string fld in fields2)
            {
                if (comma)
                    Console.Write(", ");
                else
                    comma = true;
                Console.Write("\"" + fld.ToUpper() + "\"");
            }
            Console.WriteLine(");\n");
        }

        public void drop_postgres()
        {
            Console.WriteLine("ALTER TABLE \"" + table + "\" DROP CONSTRAINT \"" + iname + "\";\n");
        }

        public void drop_microsoft()
        {
            Console.WriteLine("ALTER TABLE [" + table + "] DROP CONSTRAINT [" + iname + "]\nGO\n");
        }

    }
}
