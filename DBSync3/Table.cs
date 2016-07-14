using System;
using System.Collections.Generic;
using System.Text;

namespace DBSync3
{
    class Table : ISchemaElement
    {
        private string name;
        private LinkedList<ISchemaElement> columns;

        public Table(string nam)
        {
            name = nam;
            columns = new LinkedList<ISchemaElement>();
        }

        public void add_column(ISchemaElement col)
        {
            columns.AddLast(col);
        }

        public string get_name()
        {
            return name;
        }

        public LinkedList<ISchemaElement> get_columns()
        {
            return columns;
        }

        public ISchemaElement find(string name)
        {
            foreach (ISchemaElement col in columns)
                if (string.Compare(col.get_name(), name, true) == 0)
                    return col;
            return null;
        }

        public void add_postgres()
        {
            bool comma = false;
            Console.WriteLine("CREATE TABLE \"" + name + "\" (");
            foreach (ISchemaElement c in columns)
            {
                if (comma)
                    Console.WriteLine(",");
                else
                    comma = true;
                Console.Write("\t");
                if (c is Column)
                    ((Column)c).add_inside_postgres();
                else if (c is Check)
                    ((Check)c).add_inside_postgres();
            }
            Console.WriteLine("\n);\n");
        }

        public void add_microsoft()
        {
            bool comma = false;
            Console.WriteLine("CREATE TABLE [" + name + "] (");
            foreach (ISchemaElement c in columns)
            {
                if (comma)
                    Console.WriteLine(",");
                else
                    comma = true;
                Console.Write("\t");
                if (c is Column)
                    ((Column)c).add_inside_microsoft();
                else if (c is Check)
                    ((Check)c).add_inside_microsoft();
            }
            Console.WriteLine("\n)\nGO\n");
        }

        public void add_oracle()
        {
            bool comma = false;
            Console.WriteLine("CREATE TABLE \"" + name.ToUpper() + "\" (");
            foreach (ISchemaElement c in columns)
            {
                if (comma)
                    Console.WriteLine(",");
                else
                    comma = true;
                Console.Write("\t");
                if (c is Column)
                    ((Column)c).add_inside_oracle();
                else if (c is Check)
                    ((Check)c).add_inside_oracle();
            }
            Console.WriteLine("\n);\n");
        }

        public void drop_postgres()
        {
            Console.WriteLine("DROP TABLE \"" + name + "\";\n");
        }

        public void drop_microsoft()
        {
            Console.WriteLine("DROP TABLE \"" + name + "\"\nGO\n");
        }
    }

}
