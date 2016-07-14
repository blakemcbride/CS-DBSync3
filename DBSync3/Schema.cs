using System;
using System.Collections.Generic;
using System.Text;

namespace DBSync3
{
    class Schema : LinkedList<ISchemaElement>
    {
        public const int Postgres = 1;
        public const int Microsoft = 2;

        private int database;

        public Schema(int dbtype) : base()
        {
            database = dbtype;
        }

        public int get_dbtype()
        {
            return database;
        }

        public Table find_table(string table)
        {
            foreach (ISchemaElement item in this)
                if (item is Table && string.Compare(item.get_name(), table, true) == 0)
                    return (Table)item;
            return null;
        }

        public ISchemaElement find(string name)
        {
            foreach (ISchemaElement item in this)
                if (string.Compare(item.get_name(), name, true) == 0)
                    return item;
            return null;
        }

    }
}
